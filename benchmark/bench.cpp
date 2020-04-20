#include <errno.h>
#include <netdb.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <unistd.h>

#include <cassert>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <thread>
#include <vector>

constexpr size_t MAX_ARGS_SIZE = 1024;
constexpr uint16_t MAX_VBUCKET = 4096;

constexpr const char* RADDR = "127.0.0.1";
constexpr size_t NUM_ROUTERS = 16;
constexpr const char* RPORTS[NUM_ROUTERS] =
	{"3401", "3402", "3403", "3404", "3405", "3406", "3407", "3408",
	 "3409", "3410", "3411", "3412", "3413", "3414", "3415", "3416"};

constexpr const char* SADDR = "127.0.0.1";
constexpr size_t NUM_STORAGES = 4;
constexpr const char* SPORTS[NUM_STORAGES] =
	{"3301", "3302", "3303", "3304"};

constexpr size_t MAX_BUF = 256 * 1024;

constexpr size_t TIME_WAIT = 1000000000ull * 5;
constexpr size_t NUM_THREADS = 4;

void check(bool expr, const char* mess)
{
	if (expr)
		return;
	std::cerr << "Fatal: " << mess << std::endl;
	std::cerr << "errno: " << strerror(errno) << std::endl;
	abort();
}

struct __attribute__((packed)) MagicSizeMark
{
	uint8_t m_TotalSizeTag = 0xcd;
	uint16_t m_TotalSize = 0;
};

struct __attribute__((packed)) MagicRecvSizeMark
{
	uint8_t m_TotalSizeTag;
	uint32_t m_TotalSize = 0;
	size_t get() { return __builtin_bswap32(m_TotalSize); }
};

struct __attribute__((packed)) MagicCallCore
{
	uint8_t m_HeaderMap = 0x82;
	uint8_t m_HeaderCodeKey = 0x00; // IPROTO_REQUEST_TYPE
	uint8_t m_HeaderCodeValue = 0x0a; //  IPROTO_CALL
	uint8_t m_HeaderSyncKey = 0x01; // IPROTO_SYNC
	uint8_t m_HeaderSyncValue = 0;
	uint8_t m_BodyMap = 0x82;
	uint8_t m_BodyFuncKey = 0x22; // IPROTO_FUNCTION_NAME
	uint8_t m_BodyFuncValueTag = 0xa0 + 20;
	char m_BodyFuncValue[20] = {'v','s','h','a','r','d','.','r','o','u','t','e','r','.','c','a','l','l','r','w'};
	//char m_BodyFuncValue[20] = {'v','s','h','a','r','d','_','r','o','u','t','e','r','_','c','a','l','l','r','w'};
	uint8_t m_BodyTupleKey = 0x21; // IPROTO_TUPLE
	uint8_t m_BodyTupleValueTag = 0x94;
	uint8_t m_BucketTag = 0xd1;
	uint16_t m_Bucket = 0;
	uint8_t m_StoreFuncTag = 0xa0 + 15;
	char m_StoreFunc[15] = {'b','e','n','c','h','_','c','a','l','l','_','e','c','h','o'};
	uint8_t m_ArgsTag = 0x91;
};
//call('vshard.router.callrw', {1, 'bench_call_echo', {{1, 2, 3}}});

struct __attribute__((packed)) MagicCall
{
	MagicSizeMark m_SizeMark;
	MagicCallCore m_Core;
	char m_Args[MAX_ARGS_SIZE];
};

struct __attribute__((packed)) MagicCallTail
{
	uint8_t m_OptionsMap = 0x81;
	uint8_t m_TimeoutKeyTag = 0xa0 + 7;
	char m_TimeoutKey[7] = {'t','i','m','e','o','u','t'};
	uint8_t m_TimeoutValue = 30;
};

struct __attribute__((packed)) MagicCall2Core
{
	uint8_t m_HeaderMap = 0x82;
	uint8_t m_HeaderCodeKey = 0x00; // IPROTO_REQUEST_TYPE
	uint8_t m_HeaderCodeValue = 0x0a; //  IPROTO_CALL
	uint8_t m_HeaderSyncKey = 0x01; // IPROTO_SYNC
	uint8_t m_HeaderSyncValue = 0;
	uint8_t m_BodyMap = 0x82;
	uint8_t m_BodyFuncKey = 0x22; // IPROTO_FUNCTION_NAME
	uint8_t m_BodyFuncValueTag = 0xa0 + 15;
	char m_BodyFuncValue[15] = {'b','e','n','c','h','_','c','a','l','l','_','e','c','h','o'};
	uint8_t m_BodyTupleKey = 0x21; // IPROTO_TUPLE
	uint8_t m_BodyTupleValueTag = 0x91;
	uint8_t m_ArgsTag = 0x91;
};
//call('bench_call_echo', {{1, 2, 3}})

struct __attribute__((packed)) MagicCall2
{
	MagicSizeMark m_SizeMark;
	MagicCall2Core m_Core;
	char m_Args[MAX_ARGS_SIZE];
};

size_t generateArgs(char* data, size_t count)
{
	char *data_begin = data;
	if (count < 16)
	{
		*data = 0x90 + count;
		++data;
	}
	else
	{
		*data = 0xdc;
		++data;
		uint16_t cp = __builtin_bswap16(count);
		memcpy(data, &cp, 2);
		data += 2;
	}
	for (size_t i = 0; i < count; i++)
	{
		if (rand() % 1)
		{
			static const char strings[4][16] = {"", "a", "abc", "1234567"};
			const char* str = strings[rand() % 4];
			size_t len = strlen(str);
			*data = 0xa0;
			++data;
			memcpy(data, str, len);
			data += len;
		}
		else
		{
			int r = rand() % 4;
			if (r == 0)
			{
				*data = rand() % 128;
				++data;
			}
			else if (r == 1)
			{
				*data = 0xcc;
				++data;
				*data = uint8_t(128 +  rand() % 128);
				++data;
			}
			else if (r == 2)
			{
				*data = 0xcd;
				++data;
				uint16_t val = rand();
				while (val < 256)
					val = rand();
				val = __builtin_bswap16(val);
				memcpy(data, &val, sizeof(val));
				data += sizeof(val);
			}
			else
			{
				*data = 0xce;
				++data;
				uint32_t val = rand();
				while (val < 65536)
					val = rand();
				val = __builtin_bswap32(val);
				memcpy(data, &val, sizeof(val));
				data += sizeof(val);
			}
		}
	}
	return data - data_begin;
}

const char* generateRequest(size_t arg_count, size_t& size)
{
	thread_local MagicCall call;

	uint16_t vbucket = 1 + rand() % MAX_VBUCKET;
	call.m_Core.m_Bucket = __builtin_bswap16(vbucket);

	size = generateArgs(call.m_Args, arg_count);

	MagicCallTail tail;
	memcpy(call.m_Args + size, &tail, sizeof(tail));
	size += sizeof(tail);
	check(size <= MAX_ARGS_SIZE, "magic buff is too small");

	size += sizeof(call.m_Core);
	call.m_SizeMark.m_TotalSize = __builtin_bswap16(size);
	size += sizeof(call.m_SizeMark);

	return reinterpret_cast<char*>(&call);
}

const char* generateRequest2(size_t arg_count, size_t& size)
{
	thread_local MagicCall2 call;

	size = generateArgs(call.m_Args, arg_count);
	assert(size < MAX_ARGS_SIZE);
	size += sizeof(call.m_Core);

	call.m_SizeMark.m_TotalSize = __builtin_bswap16(size);
	size += sizeof(call.m_SizeMark);

	return reinterpret_cast<char*>(&call);
}

class AddrInfo
{
public:
	AddrInfo(const char* aAddress, const char* aPort)
	{
		struct addrinfo hints{};
		hints.ai_family = AF_UNSPEC;
		hints.ai_socktype = SOCK_STREAM;

		int rc = getaddrinfo(aAddress, aPort, &hints, &m_Info);
		check(rc == 0, "getaddrinfo");
	}

	~AddrInfo() noexcept
	{
		freeaddrinfo(m_Info);
	}

	class iterator : std::iterator<std::input_iterator_tag, const struct addrinfo>
	{
	private:
		using T = const struct addrinfo;
	public:
		T& operator*() const { return *m_Info; }
		T* operator->() const { return m_Info; }
		bool operator==(const iterator& aItr) { return m_Info == aItr.m_Info; }
		bool operator!=(const iterator& aItr) { return m_Info != aItr.m_Info; }
		iterator& operator++() { m_Info = m_Info->ai_next; return *this; }
		iterator operator++(int) { iterator sTmp = *this; m_Info = m_Info->ai_next; return sTmp; }

	private:
		friend class AddrInfo;
		explicit iterator(const struct addrinfo* aInfo) : m_Info(aInfo) {}
		T *m_Info;
	};

	iterator begin() const { return iterator(m_Info); }
	iterator end() const { return iterator(nullptr); }

private:
	struct addrinfo* m_Info;
};


struct CycleBuf
{
	struct IOV
	{
		struct iovec vec[2];
		size_t count;
	};

	size_t beg = 0;
	size_t fin = 0;
	char buf[MAX_BUF];

	size_t used() const
	{
		return fin - beg;
	}
	size_t free() const
	{
		return MAX_BUF - used();
	}
	bool empty() const
	{
		return beg == fin;
	}
	void reset()
	{
		beg = fin = 0;
	}
	void append(const char* data, size_t size)
	{
		check(size <= free(), "buf append");
		size_t bpos = fin % MAX_BUF;
		fin += size;
		size_t part_size = MAX_BUF - bpos;
		if (size <= part_size)
		{
			memcpy(buf + bpos, data, size);
		}
		else
		{
			memcpy(buf + bpos, data, part_size);
			memcpy(buf, data + part_size, size - part_size);
		}
	}
	void drop(size_t size)
	{
		beg += size;
	}
	IOV toRecv()
	{
		size_t size = free();
		check(size > 0, "buf no space");
		size_t bpos = fin % MAX_BUF;
		IOV res;
		res.vec[0].iov_base = buf + bpos;
		if (size <= MAX_BUF - bpos)
		{
			res.vec[0].iov_len = size;
			res.count = 1;
		}
		else
		{
			res.vec[0].iov_len = MAX_BUF - bpos;
			res.vec[1].iov_base = buf;
			res.vec[1].iov_len = size - res.vec[0].iov_len;
			res.count = 2;
		}
		return res;
	}
	void recvDone(size_t size)
	{
		check(size <= free(), "buf wrong recv");
		fin += size;
	}
	IOV toSend()
	{
		size_t size = used();
		check(size > 0, "buf nothing to send");
		size_t bpos = beg % MAX_BUF;
		IOV res;
		res.vec[0].iov_base = buf + bpos;
		if (size <= MAX_BUF - bpos)
		{
			res.vec[0].iov_len = size;
			res.count = 1;
		}
		else
		{
			res.vec[0].iov_len = MAX_BUF - bpos;
			res.vec[1].iov_base = buf;
			res.vec[1].iov_len = size - res.vec[0].iov_len;
			res.count = 2;
		}
		return res;
	}
	void sendDone(size_t size)
	{
		check(size <= used(), "buf wrong send");
		beg += size;
	}
	void peek(char* peek, size_t size)
	{
		check(size <= used(), "buf wrong peak");
		size_t bpos = beg % MAX_BUF;
		if (size <= MAX_BUF - bpos)
		{
			memcpy(peek, buf + bpos, size);
		}
		else
		{
			memcpy(peek, buf + bpos, MAX_BUF - bpos);
			peek += MAX_BUF - bpos;
			size -= MAX_BUF - bpos;
			memcpy(peek, buf, size);
		}
	}
};

enum srv_type { STORAGE, ROUTER };

struct Conn
{
	int fd = -1;
	bool greet_done = false;
	CycleBuf ibuf;
	CycleBuf obuf;
	size_t processed = 0;
	Conn() = default;
	~Conn() { if (fd >= 0) close(fd); }
	void add_req(size_t tuple_size, srv_type type)
	{
		size_t buf_size;
		const char* buf = type == ROUTER ?
		                  generateRequest(tuple_size, buf_size) :
		                  generateRequest2(tuple_size, buf_size);
		obuf.append(buf, buf_size);
	}
	void init(const char* addr, const char* port, size_t num_req, size_t tuple_size, srv_type type)
	{
		for (auto sInfo : AddrInfo(addr, port))
		{
			fd = socket(sInfo.ai_family, sInfo.ai_socktype, sInfo.ai_protocol);
			if (fd < 0)
				continue;
			if (0 != connect(fd, sInfo.ai_addr, sInfo.ai_addrlen))
			{
				close(fd);
				fd = -1;
				continue;
			}
			break;
		}
		check(fd >= 0, "connect");
		for (size_t i = 0; i < num_req; i++)
			add_req(tuple_size, type);
	}
};

static unsigned long long now()
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return ((unsigned long long)ts.tv_sec) * 1000000000ull + ts.tv_nsec;
}

void test_f(size_t num_srv, size_t from_srv, size_t num_conns, size_t req_per_conn, size_t tuple_size, srv_type type, double* krps)
{
	const char *addr = type == STORAGE ? SADDR : RADDR;
	const char *const*ports = type == STORAGE ? SPORTS : RPORTS;
	std::vector<Conn> conns(num_conns);
	for (size_t i = 0; i < num_conns; i++)
		conns[i].init(addr, ports[(i + from_srv) % num_srv], req_per_conn, tuple_size, type);
	//int efd = epoll_create1(0);
	int efd = epoll_create(1);
	check(efd >= 0, "epoll_create");
	for (size_t i = 0; i < num_conns; i++)
	{
		epoll_event ev{};
		ev.data.u64 = i;
		ev.events = EPOLLIN | EPOLLOUT;
		int r = epoll_ctl(efd, EPOLL_CTL_ADD, conns[i].fd, &ev);
		check(r == 0, "epoll_ctl add");
	}

	size_t total_req = 0;
	unsigned long long start = now();
	unsigned long long fin = start + TIME_WAIT;
	unsigned long long cur = start;

	const size_t NUM_EV = 128;
	epoll_event evs[NUM_EV];
	while (1)
	{
		int n = epoll_wait(efd, evs, NUM_EV, 10);
		check(n >= 0, "epoll_wait");

		for (int j = 0; j < n; j++)
		{
			check((evs[j].events & (EPOLLERR | EPOLLHUP)) == 0, "EPOLLERR | EPOLLHUP");
			Conn& conn = conns[evs[j].data.u64];
			if (evs[j].events & EPOLLIN)
			{
				CycleBuf::IOV iov = conn.ibuf.toRecv();
				struct msghdr hdr{};
				hdr.msg_iov = iov.vec;
				hdr.msg_iovlen = iov.count;

				ssize_t r = recvmsg(conn.fd, &hdr, MSG_NOSIGNAL | MSG_DONTWAIT);
				check(r > 0 || (r < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)), "recvmsg");
				if (r > 0)
				{
					conn.ibuf.recvDone(r);
					if (!conn.greet_done && conn.ibuf.used() >= 128)
					{
						conn.greet_done = true;
						conn.ibuf.drop(128);
					}
					while (conn.greet_done && conn.ibuf.used() >= sizeof(MagicRecvSizeMark))
					{
						MagicRecvSizeMark mark{};
						conn.ibuf.peek(reinterpret_cast<char*>(&mark), sizeof(mark));
						check(mark.m_TotalSizeTag == 0xce, "wrong reply");
						size_t repl_size = mark.get() + sizeof(mark);
						if (conn.ibuf.used() < repl_size)
							break;
						total_req++;
						conn.processed++;
						conn.ibuf.drop(repl_size);
						if (conn.obuf.empty())
						{
							epoll_event ev{};
							ev.data.u64 = evs[j].data.u64;
							ev.events = EPOLLIN | EPOLLOUT;
							int r = epoll_ctl(efd, EPOLL_CTL_MOD, conn.fd, &ev);
							check(r == 0, "epoll_ctl mod 1");
						}
						conn.add_req(tuple_size, type);
					}
				}
			}
			if (evs[j].events & EPOLLOUT)
			{
				CycleBuf::IOV iov = conn.obuf.toSend();
				struct msghdr hdr{};
				hdr.msg_iov = iov.vec;
				hdr.msg_iovlen = iov.count;

				ssize_t r = sendmsg(conn.fd, &hdr, MSG_DONTWAIT);
				check(r > 0 || (r < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)), "sendmsg");
				if (r > 0)
				{
					conn.obuf.sendDone(r);
					if (conn.obuf.empty())
					{
						epoll_event ev{};
						ev.data.u64 = evs[j].data.u64;
						ev.events = EPOLLIN;
						int r = epoll_ctl(efd, EPOLL_CTL_MOD, conn.fd, &ev);
						check(r == 0, "epoll_ctl mod 2");
					}
				}
			}
		}
		cur = now();
		if (cur > fin)
			break;
	}

	close(efd);

	double kr = total_req;
	kr /= 1000;
	double tm = cur - start;
	tm /= 1000000000;
	*krps = kr / tm;
}

void test(size_t num_srv, size_t num_conns, size_t req_per_conn, size_t tuple_size, srv_type type)
{
	check(num_conns % num_srv == 0, "unbalanced workload");
	size_t num_threads = std::min(NUM_THREADS, num_conns);
	check(num_conns % num_threads == 0, "can't balance threads");
	size_t conn_per_thread = num_conns / num_threads;
	std::vector<std::thread> thr(num_threads);
	std::vector<double> results(num_threads);
	for (size_t i = 0; i < num_threads; i++)
		thr[i] = std::thread(test_f, num_srv, i * conn_per_thread, conn_per_thread, req_per_conn, tuple_size, type, &results[i]);

	for (size_t i = 0; i < num_threads; i++)
		thr[i].join();

	double krps = 0;
	for (size_t i = 0; i < num_threads; i++)
		krps += results[i];

	printf("|    %4lu    |    %4lu    |    %4lu    |    %4lu    |   %6.1lf   |\n",
	       num_srv, num_conns, req_per_conn, tuple_size, krps);
}

int main()
{
	printf("note: 'connections' - total per test; each router receives 'connections' / 'routers'.\n");

	printf("\n");
	printf("|  storages  |connections |batch factor| tuple size |    krps    |\n");
	printf("|------------|------------|------------|------------|------------|\n");
	for (size_t i = 1; i <= 1024; i *= 2)
		test(1, 1024 / i, i, 0, STORAGE);
	for (size_t i = 1; i <= NUM_STORAGES; i *= 2)
		test(i, 32, 32, 0, STORAGE);
	for (size_t i = 0; i <= 50; i += 10)
		test(1, 32, 32, i, STORAGE);
	for (size_t i = 0; i <= 50; i += 10)
		test(NUM_STORAGES, 32, 32, i, STORAGE);

	printf("\n");
	printf("|  routers   |connections |batch factor| tuple size |    krps    |\n");
	printf("|------------|------------|------------|------------|------------|\n");
	for (size_t i = 1; i <= 1024; i *= 2)
		test(1, 1024 / i, i, 0, ROUTER);
	for (size_t i = 1; i <= NUM_ROUTERS; i *= 2)
		test(i, 32, 32, 0, ROUTER);
	for (size_t i = 0; i <= 50; i += 10)
		test(1, 32, 32, i, ROUTER);
	for (size_t i = 0; i <= 50; i += 10)
		test(NUM_ROUTERS, 32, 32, i, ROUTER);
}
