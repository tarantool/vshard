#ifndef TARANTOOL_MODULE_H_INCLUDED
#define TARANTOOL_MODULE_H_INCLUDED

/*
 * Copyright 2010-2016, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY AUTHORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/**
 * \file
 */

#include <stddef.h>
#include <stdarg.h> /* va_list */
#include <errno.h>
#include <string.h> /* strerror(3) */
#include <stdint.h>
#include <stdbool.h>
#include <stdio.h> /* ssize_t for Apple */
#include <sys/types.h> /* ssize_t */

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

#include "lua.h"  /* does not have extern C wrappers */
/** \cond public */

/**
 * Package major version - 1 for 1.6.7
 */
#define PACKAGE_VERSION_MAJOR 2
/**
 * Package minor version - 6 for 1.6.7
 */
#define PACKAGE_VERSION_MINOR 5
/**
 * Package patch version - 7 for 1.6.7
 */
#define PACKAGE_VERSION_PATCH 0
/**
 * A string with major-minor-patch-commit-id identifier of the
 * release, e.g. 1.6.6-113-g8399d0e.
 */
#define PACKAGE_VERSION "2.5.0-73-g5ca12a4"

/** \endcond public */
/** \cond public */

/** System configuration dir (e.g /etc) */
#define SYSCONF_DIR "etc"
/** Install prefix (e.g. /usr) */
#define INSTALL_PREFIX "/usr/local"
/** Build type, e.g. Debug or Release */
#define BUILD_TYPE "Debug"
/** CMake build type signature, e.g. Linux-x86_64-Debug */
#define BUILD_INFO "Linux-x86_64-Debug"
/** Command line used to run CMake */
#define BUILD_OPTIONS "cmake . -DCMAKE_INSTALL_PREFIX=/usr/local -DENABLE_BACKTRACE=ON"
/** Pathes to C and CXX compilers */
#define COMPILER_INFO "/usr/bin/cc /usr/bin/c++"
/** C compile flags used to build Tarantool */
#define TARANTOOL_C_FLAGS " -fexceptions -funwind-tables -fno-omit-frame-pointer -fno-stack-protector -fno-common -fopenmp -msse2 -std=c11 -Wall -Wextra -Wno-strict-aliasing -Wno-char-subscripts -Wno-format-truncation -fno-gnu89-inline -Wno-cast-function-type -Werror"
/** CXX compile flags used to build Tarantool */
#define TARANTOOL_CXX_FLAGS " -fexceptions -funwind-tables -fno-omit-frame-pointer -fno-stack-protector -fno-common -fopenmp -msse2 -std=c++11 -Wall -Wextra -Wno-strict-aliasing -Wno-char-subscripts -Wno-format-truncation -Wno-invalid-offsetof -Wno-cast-function-type -Werror"

/** A path to install *.lua module files */
#define MODULE_LIBDIR "/usr/local/lib/tarantool"
/** A path to install *.so / *.dylib module files */
#define MODULE_LUADIR "/usr/local/share/tarantool"
/** A path to Lua includes (the same directory where this file is contained) */
#define MODULE_INCLUDEDIR "/usr/local/include/tarantool"
/** A constant added to package.path in Lua to find *.lua module files */
#define MODULE_LUAPATH "/usr/local/share/tarantool/?.lua;/usr/local/share/tarantool/?/init.lua;/usr/share/tarantool/?.lua;/usr/share/tarantool/?/init.lua;/usr/local/share/lua/5.1/?.lua;/usr/local/share/lua/5.1/?/init.lua;/usr/share/lua/5.1/?.lua;/usr/share/lua/5.1/?/init.lua"
/** A constant added to package.cpath in Lua to find *.so module files */
#define MODULE_LIBPATH "/usr/local/lib/x86_64-linux-gnu/tarantool/?.so;/usr/local/lib/tarantool/?.so;/usr/lib/x86_64-linux-gnu/tarantool/?.so;/usr/local/lib/x86_64-linux-gnu/lua/5.1/?.so;/usr/local/lib/lua/5.1/?.so;/usr/lib/x86_64-linux-gnu/lua/5.1/?.so"
/** Shared library suffix - ".so" on Linux, ".dylib" on Mac */
#define MODULE_LIBSUFFIX ".so"

/** \endcond public */
/** \cond public */

/**
 * Feature test macroses for -std=c11 / -std=c++11
 *
 * Sic: clang aims to be gcc-compatible and thus defines __GNUC__
 */
#ifndef __has_feature
#  define __has_feature(x) 0
#endif
#ifndef __has_builtin
#  define __has_builtin(x) 0
#endif
#ifndef __has_attribute
#  define __has_attribute(x) 0
#endif
#ifndef __has_cpp_attribute
#  define __has_cpp_attribute(x) 0
#endif

/**
 * Compiler-independent built-ins.
 *
 * \see https://gcc.gnu.org/onlinedocs/gcc/Other-Builtins.html
 *
 * {{{ Built-ins
 */

/**
 * You may use likely()/unlikely() to provide the compiler with branch
 * prediction information.
 */

#if __has_builtin(__builtin_expect) || defined(__GNUC__)
#  define likely(x)    __builtin_expect(!! (x),1)
#  define unlikely(x)  __builtin_expect(!! (x),0)
#else
#  define likely(x)    (x)
#  define unlikely(x)  (x)
#endif

/**
 * This macro is used to minimize cache-miss latency by moving data into
 * a cache before it is accessed. You can insert calls to prefetch() into
 * code for which you know addresses of data in memory that is likely to be
 * accessed soon. If the target supports them, data prefetch instructions
 * will be generated. If the prefetch is done early enough before the access
 * then the data will be in the cache by the time it is accessed.
 *
 * The value of addr is the address of the memory to prefetch. There are two
 * optional arguments, rw and locality. The value of rw is a compile-time
 * constant one or zero; one means that the prefetch is preparing for a write
 * to the memory address and zero, the default, means that the prefetch is
 * preparing for a read. The value locality must be a compile-time constant
 * integer between zero and three. A value of zero means that the data has
 * no temporal locality, so it need not be left in the cache after the access.
 * A value of three means that the data has a high degree of temporal locality
 * and should be left in all levels of cache possible. Values of one and two
 * mean, respectively, a low or moderate degree of temporal locality.
 * The default is three.
 */
#if __has_builtin(__builtin_prefetch) || defined(__GNUC__)
#  define prefetch(addr, ...) (__builtin_prefetch(addr, __VA_ARGS__))
#else
#  define prefetch(addr, ...) ((void) addr)
#endif

/**
 * If control flow reaches the point of the unreachable(), the program is
 * undefined. It is useful in situations where the compiler cannot deduce
 * the unreachability of the code.
 */
//#if __has_builtin(__builtin_unreachable) || defined(__GNUC__)
//#  define unreachable() (assert(0), __builtin_unreachable())
//#else
//#  define unreachable() (assert(0))
//#endif

/**
 * The macro offsetof expands to an integral constant expression of
 * type size_t, the value of which is the offset, in bytes, from
 * the beginning of an object of specified type to its specified member,
 * including padding if any.
 */
#ifndef offsetof
#define offsetof(type, member) ((size_t) &((type *)0)->member)
#endif

/**
 * This macro is used to retrieve an enclosing structure from a pointer to
 * a nested element.
 */
#ifndef container_of
#define container_of(ptr, type, member) ({ \
	const typeof( ((type *)0)->member  ) *__mptr = (ptr); \
	(type *)( (char *)__mptr - offsetof(type,member)  );})
#endif

/**
 * C11/C++11 keyword. Appears in the declaration syntax as one of the type
 * specifiers to modify the alignment requirement of the object being
 * declared.
 *
 * Sic: alignas() doesn't work on anonymous strucrs on gcc < 4.9
 *
 * \example struct obuf { int a; int b; alignas(16) int c; };
 */
#if !defined(alignas) && !defined(__alignas_is_defined)
#  if __has_feature(c_alignas) || (defined(__GNUC__) && __GNUC__ >= 5)
#    include <stdalign.h>
#  elif __has_attribute(aligned) || defined(__GNUC__)
#    define alignas(_n) __attribute__((aligned(_n)))
#    define __alignas_is_defined 1
#  else
#    define alignas(_n)
#  endif
#endif

/**
 * C11/C++11 operator. Returns the alignment, in bytes, required for any
 * instance of the type indicated by type-id, which is either complete type,
 * an array type, or a reference type.
 */
#if !defined(alignof) && !defined(__alignof_is_defined)
#  if __has_feature(c_alignof) || (defined(__GNUC__) && __GNUC__ >= 5)
#    include <stdalign.h>
#  elif defined(__GNUC__)
#    define alignof(_T) __alignof(_T)
#    define __alignof_is_defined 1
#  else
#    define alignof(_T) offsetof(struct { char c; _T member; }, member)
#    define __alignof_is_defined 1
#  endif
#endif

/** Built-ins }}} */

/**
 * Compiler-indepedent function attributes.
 *
 * \see https://gcc.gnu.org/onlinedocs/gcc/Type-Attributes.html
 * \see http://clang.llvm.org/docs/AttributeReference.html#function-attributes
 * \see http://en.cppreference.com/w/cpp/language/attributes
 *
 * {{{ Function Attributes
 */

/**
 * The MAYBE_UNUSED function attribute can be used to silence -Wunused
 * diagnostics when the entity cannot be removed. For instance, a local
 * variable may exist solely for use in an assert() statement, which makes
 * the local variable unused when NDEBUG is defined.
 *
 * \example int fun(MAYBE_UNUSED int unused_arg);
 */
#if defined(__cplusplus) && __has_cpp_attribute(maybe_unused)
#  define MAYBE_UNUSED [[maybe_unused]]
#elif __has_attribute(unused) || defined(__GNUC__)
#  define MAYBE_UNUSED __attribute__((unused))
#else
#  define MAYBE_UNUSED
#endif

/**
 * A diagnostic is generated when a function is marked with NODISCARD and
 * the function call appears as a potentially-evaluated discarded-value
 * expression that is not explicitly cast to void.
 *
 * \example NODISCARD int function() { return -1 };
 */
#if defined(__cplusplus) && __has_cpp_attribute(nodiscard)
#  define NODISCARD [[nodiscard]]
#elif __has_attribute(warn_unused_result) || defined(__GNUC__)
#  define NODISCARD __attribute__((warn_unused_result))
#else
#  define NODISCARD
#endif

/**
 * This function attribute prevents a function from being considered for
 * inlining.
 *
 * \example NOINLINE int function() { return 0; };
 */
#if __has_attribute(noinline) || defined(__GNUC__)
#  define NOINLINE __attribute__((noinline))
#else
#  define NOINLINE
#endif

/**
 * A function declared as NORETURN shall not return to its caller.
 * The compiler will generate a diagnostic for a function declared as
 * NORETURN that appears to be capable of returning to its caller.
 *
 * \example NORETURN void abort();
 */
#if defined(__cplusplus) && __has_cpp_attribute(noreturn)
#  define NORETURN [[noreturn]]
#elif __has_attribute(noreturn) || defined(__GNUC__)
#  define NORETURN  __attribute__((noreturn))
#else
#  define NORETURN
#endif

/**
 * The DEPRECATED attribute can be applied to a function, a variable, or
 * a type. This is useful when identifying functions, variables, or types
 * that are expected to be removed in a future version of a program.
 */
#if defined(__cplusplus) && __has_cpp_attribute(deprecated)
#  define DEPRECATED(_msg) [[deprecated(_msg)]]
#elif __has_attribute(deprecated) || defined(__GNUC__)
#  define DEPREACTED  __attribute__((deprecated(_msg)))
#else
#  define DEPRECATED(_msg)
#endif

/**
 * The API_EXPORT attribute declares public C API function.
 */
#if defined(__cplusplus) && defined(__GNUC__)
#  define API_EXPORT extern "C" __attribute__ ((nothrow, visibility ("default")))
#elif defined(__cplusplus)
#  define API_EXPORT extern "C"
#elif defined(__GNUC__)
#  define API_EXPORT extern __attribute__ ((nothrow, visibility ("default")))
#else
#  define API_EXPORT extern
#endif

/**
 * The CFORMAT attribute specifies that a function takes printf, scanf,
 * strftime or strfmon style arguments that should be type-checked against
 * a format string.
 *
 * \see https://gcc.gnu.org/onlinedocs/gcc/Common-Function-Attributes.html
 */
#if __has_attribute(format) || defined(__GNUC__)
#  define CFORMAT(_archetype, _stringindex, _firsttocheck) \
	__attribute__((format(_archetype, _stringindex, _firsttocheck)))
#else
#  define CFORMAT(archetype, stringindex, firsttocheck)
#endif

/**
 * The PACKED qualifier is useful to map a structure to an external data
 * structure, or for accessing unaligned data, but it is generally not
 * useful to save data size because of the relatively high cost of
 * unaligned access on some architectures.
 *
 * \example struct PACKED name { char a; int b; };
 */
#if __has_attribute(packed) || defined(__GNUC__)
#  define PACKED  __attribute__((packed))
#elif defined(__CC_ARM)
#  define PACKED __packed
#else
#  define PACKED
#endif

/** Function Attributes }}} */

/** {{{ Statement Attributes */

/**
 * The fallthrough attribute with a null statement serves as a fallthrough
 * statement. It hints to the compiler that a statement that falls through
 * to another case label, or user-defined label in a switch statement is
 * intentional and thus the -Wimplicit-fallthrough warning must not trigger.
 * The fallthrough attribute may appear at most once in each attribute list,
 * and may not be mixed with other attributes. It can only be used in a switch
 * statement (the compiler will issue an error otherwise), after a preceding
 * statement and before a logically succeeding case label, or user-defined
 * label.
 */
#if defined(__cplusplus) && __has_cpp_attribute(fallthrough)
#  define FALLTHROUGH [[fallthrough]]
#elif __has_attribute(fallthrough) || (defined(__GNUC__) && __GNUC__ >= 7)
#  define FALLTHROUGH __attribute__((fallthrough))
#else
#  define FALLTHROUGH
#endif

/** Statement Attributes }}} */

/** \endcond public */
/** \cond public */

/** Log levels */
enum say_level {
	S_FATAL,		/* do not use this value directly */
	S_SYSERROR,
	S_ERROR,
	S_CRIT,
	S_WARN,
	S_INFO,
	S_VERBOSE,
	S_DEBUG
};

/** Log formats */
enum say_format {
	SF_PLAIN,
	SF_JSON,
	say_format_MAX
};

extern int log_level;

static inline bool
say_log_level_is_enabled(int level)
{
       return level <= log_level;
}

/** \endcond public */
/** \cond public */
typedef void (*sayfunc_t)(int, const char *, int, const char *,
		    const char *, ...);

/** Internal function used to implement say() macros */
CFORMAT(printf, 5, 0) extern sayfunc_t _say;

/**
 * Format and print a message to Tarantool log file.
 *
 * \param level (int) - log level (see enum \link say_level \endlink)
 * \param file (const char * ) - file name to print
 * \param line (int) - line number to print
 * \param error (const char * ) - error description, may be NULL
 * \param format (const char * ) - printf()-like format string
 * \param ... - format arguments
 * \sa printf()
 * \sa enum say_level
 */
#define say_file_line(level, file, line, error, format, ...) ({ \
	if (say_log_level_is_enabled(level)) \
		_say(level, file, line, error, format, ##__VA_ARGS__); })

/**
 * Format and print a message to Tarantool log file.
 *
 * \param level (int) - log level (see enum \link say_level \endlink)
 * \param error (const char * ) - error description, may be NULL
 * \param format (const char * ) - printf()-like format string
 * \param ... - format arguments
 * \sa printf()
 * \sa enum say_level
 */
#define say(level, error, format, ...) ({ \
	say_file_line(level, __FILE__, __LINE__, error, format, ##__VA_ARGS__); })

/**
 * Format and print a message to Tarantool log file.
 *
 * \param format (const char * ) - printf()-like format string
 * \param ... - format arguments
 * \sa printf()
 * \sa enum say_level
 * Example:
 * \code
 * say_info("Some useful information: %s", status);
 * \endcode
 */
#define say_error(format, ...) say(S_ERROR, NULL, format, ##__VA_ARGS__)
/** \copydoc say_error() */
#define say_crit(format, ...) say(S_CRIT, NULL, format, ##__VA_ARGS__)
/** \copydoc say_error() */
#define say_warn(format, ...) say(S_WARN, NULL, format, ##__VA_ARGS__)
/** \copydoc say_error() */
#define say_info(format, ...) say(S_INFO, NULL, format, ##__VA_ARGS__)
/** \copydoc say_error() */
#define say_verbose(format, ...) say(S_VERBOSE, NULL, format, ##__VA_ARGS__)
/** \copydoc say_error() */
#define say_debug(format, ...) say(S_DEBUG, NULL, format, ##__VA_ARGS__)
/** \copydoc say_error(). */
#define say_syserror(format, ...) say(S_SYSERROR, strerror(errno), format, \
	##__VA_ARGS__)
/** \endcond public */
/** \cond public */

/**
 * Fiber attributes container
 */
struct fiber_attr;

/**
 * Create a new fiber attribute container and initialize it
 * with default parameters.
 * Can be used for many fibers creation, corresponding fibers
 * will not take ownership.
 */
API_EXPORT struct fiber_attr *
fiber_attr_new();

/**
 * Delete the fiber_attr and free all allocated resources.
 * This is safe when fibers created with this attribute still exist.
 *
 *\param fiber_attr fiber attribute
 */
API_EXPORT void
fiber_attr_delete(struct fiber_attr *fiber_attr);

/**
 * Set stack size for the fiber attribute.
 *
 * \param fiber_attribute fiber attribute container
 * \param stacksize stack size for new fibers
 */
API_EXPORT int
fiber_attr_setstacksize(struct fiber_attr *fiber_attr, size_t stack_size);

/**
 * Get stack size from the fiber attribute.
 *
 * \param fiber_attribute fiber attribute container or NULL for default
 * \retval stack size
 */
API_EXPORT size_t
fiber_attr_getstacksize(struct fiber_attr *fiber_attr);

struct fiber;
/**
 * Fiber - contains information about fiber
 */

typedef int (*fiber_func)(va_list);

/**
 * Return the current fiber
 */
API_EXPORT struct fiber *
fiber_self();

/**
 * Create a new fiber.
 *
 * Takes a fiber from fiber cache, if it's not empty.
 * Can fail only if there is not enough memory for
 * the fiber structure or fiber stack.
 *
 * The created fiber automatically returns itself
 * to the fiber cache when its "main" function
 * completes.
 *
 * \param name       string with fiber name
 * \param fiber_func func for run inside fiber
 *
 * \sa fiber_start
 */
API_EXPORT struct fiber *
fiber_new(const char *name, fiber_func f);

/**
 * Create a new fiber with defined attributes.
 *
 * Can fail only if there is not enough memory for
 * the fiber structure or fiber stack.
 *
 * The created fiber automatically returns itself
 * to the fiber cache if has default stack size
 * when its "main" function completes.
 *
 * \param name       string with fiber name
 * \param fiber_attr fiber attributes
 * \param fiber_func func for run inside fiber
 *
 * \sa fiber_start
 */
API_EXPORT struct fiber *
fiber_new_ex(const char *name, const struct fiber_attr *fiber_attr, fiber_func f);

/**
 * Return control to another fiber and wait until it'll be woken.
 *
 * \sa fiber_wakeup
 */
API_EXPORT void
fiber_yield(void);

/**
 * Start execution of created fiber.
 *
 * \param callee fiber to start
 * \param ...    arguments to start the fiber with
 *
 * \sa fiber_new
 */
API_EXPORT void
fiber_start(struct fiber *callee, ...);

/**
 * Interrupt a synchronous wait of a fiber
 *
 * \param f fiber to be woken up
 */
API_EXPORT void
fiber_wakeup(struct fiber *f);

/**
 * Cancel the subject fiber. (set FIBER_IS_CANCELLED flag)
 *
 * If target fiber's flag FIBER_IS_CANCELLABLE set, then it would
 * be woken up (maybe prematurely). Then current fiber yields
 * until the target fiber is dead (or is woken up by
 * \sa fiber_wakeup).
 *
 * \param f fiber to be cancelled
 */
API_EXPORT void
fiber_cancel(struct fiber *f);

/**
 * Make it possible or not possible to wakeup the current
 * fiber immediately when it's cancelled.
 *
 * @param yesno status to set
 * @return previous state.
 */
API_EXPORT bool
fiber_set_cancellable(bool yesno);

/**
 * Set fiber to be joinable (false by default).
 * \param yesno status to set
 */
API_EXPORT void
fiber_set_joinable(struct fiber *fiber, bool yesno);

/**
 * Wait until the fiber is dead and then move its execution
 * status to the caller.
 * The fiber must not be detached (@sa fiber_set_joinable()).
 * @pre FIBER_IS_JOINABLE flag is set.
 *
 * \param f fiber to be woken up
 * \return fiber function ret code
 */
API_EXPORT int
fiber_join(struct fiber *f);

/**
 * Put the current fiber to sleep for at least 's' seconds.
 *
 * \param s time to sleep
 *
 * \note this is a cancellation point (\sa fiber_is_cancelled)
 */
API_EXPORT void
fiber_sleep(double s);

/**
 * Check current fiber for cancellation (it must be checked
 * manually).
 */
API_EXPORT bool
fiber_is_cancelled();

/**
 * Report loop begin time as double (cheap).
 * Uses real time clock.
 */
API_EXPORT double
fiber_time(void);

/**
 * Report loop begin time as 64-bit int.
 * Uses real time clock.
 */
API_EXPORT uint64_t
fiber_time64(void);

/**
 * Report loop begin time as double (cheap).
 * Uses monotonic clock.
 */
API_EXPORT double
fiber_clock(void);

/**
 * Report loop begin time as 64-bit int.
 * Uses monotonic clock.
 */
API_EXPORT uint64_t
fiber_clock64(void);

/**
 * Reschedule fiber to end of event loop cycle.
 */
API_EXPORT void
fiber_reschedule(void);

/**
 * Return slab_cache suitable to use with tarantool/small library
 */
struct slab_cache;
API_EXPORT struct slab_cache *
cord_slab_cache(void);

/** \endcond public */
/** \cond public */

/**
 * Conditional variable for cooperative multitasking (fibers).
 *
 * A cond (short for "condition variable") is a synchronization primitive
 * that allow fibers to yield until some predicate is satisfied. Fiber
 * conditions have two basic operations - wait() and signal(). wait()
 * suspends execution of fiber (i.e. yields) until signal() is called.
 * Unlike pthread_cond, fiber_cond doesn't require mutex/latch wrapping.
 * 
 */
struct fiber_cond;

/** \endcond public */
/** \cond public */

/**
 * Instantiate a new fiber cond object.
 */
struct fiber_cond *
fiber_cond_new(void);

/**
 * Delete the fiber cond object.
 * Behaviour is undefined if there are fiber waiting for the cond.
 */
void
fiber_cond_delete(struct fiber_cond *cond);

/**
 * Wake one fiber waiting for the cond.
 * Does nothing if no one is waiting.
 * @param cond condition
 */
void
fiber_cond_signal(struct fiber_cond *cond);

/**
 * Wake up all fibers waiting for the cond.
 * @param cond condition
 */
void
fiber_cond_broadcast(struct fiber_cond *cond);

/**
 * Suspend the execution of the current fiber (i.e. yield) until
 * fiber_cond_signal() is called. Like pthread_cond, fiber_cond can issue
 * spurious wake ups caused by explicit fiber_wakeup() or fiber_cancel()
 * calls. It is highly recommended to wrap calls to this function into a loop
 * and check an actual predicate and fiber_testcancel() on every iteration.
 *
 * @param cond condition
 * @param timeout timeout in seconds
 * @retval 0 on fiber_cond_signal() call or a spurious wake up
 * @retval -1 on timeout, diag is set to TimedOut
 */
int
fiber_cond_wait_timeout(struct fiber_cond *cond, double timeout);

/**
 * Shortcut for fiber_cond_wait_timeout().
 * @see fiber_cond_wait_timeout()
 */
int
fiber_cond_wait(struct fiber_cond *cond);

/** \endcond public */
/** \cond public */

enum {
	/** READ event */
	COIO_READ  = 0x1,
	/** WRITE event */
	COIO_WRITE = 0x2,
};

/**
 * Wait until READ or WRITE event on socket (\a fd). Yields.
 * \param fd - non-blocking socket file description
 * \param events - requested events to wait.
 * Combination of TNT_IO_READ | TNT_IO_WRITE bit flags.
 * \param timeoout - timeout in seconds.
 * \retval 0 - timeout
 * \retval >0 - returned events. Combination of TNT_IO_READ | TNT_IO_WRITE
 * bit flags.
 */
API_EXPORT int
coio_wait(int fd, int event, double timeout);

/**
 * Close the fd and wake any fiber blocked in
 * coio_wait() call on this fd.
 */
API_EXPORT int
coio_close(int fd);

/** \endcond public */
/** \cond public */

/**
 * Create new eio task with specified function and
 * arguments. Yield and wait until the task is complete.
 *
 * This function doesn't throw exceptions to avoid double error
 * checking: in most cases it's also necessary to check the return
 * value of the called function and perform necessary actions. If
 * func sets errno, the errno is preserved across the call.
 *
 * @retval -1 and errno = ENOMEM if failed to create a task
 * @retval the function return (errno is preserved).
 *
 * @code
 *	static ssize_t openfile_cb(va_list ap)
 *	{
 *	         const char *filename = va_arg(ap);
 *	         int flags = va_arg(ap);
 *	         return open(filename, flags);
 *	}
 *
 *	if (coio_call(openfile_cb, "/tmp/file", 0) == -1)
 *		// handle errors.
 *	...
 * @endcode
 */
ssize_t
coio_call(ssize_t (*func)(va_list), ...);

struct addrinfo;

/**
 * Fiber-friendly version of getaddrinfo(3).
 *
 * @param host host name, i.e. "tarantool.org"
 * @param port service name, i.e. "80" or "http"
 * @param hints hints, see getaddrinfo(3)
 * @param res[out] result, see getaddrinfo(3)
 * @param timeout timeout
 * @retval  0 on success, please free @a res using freeaddrinfo(3).
 * @retval -1 on error, check diag.
 *            Please note that the return value is not compatible with
 *            getaddrinfo(3).
 * @sa getaddrinfo()
 */
int
coio_getaddrinfo(const char *host, const char *port,
		 const struct addrinfo *hints, struct addrinfo **res,
		 double timeout);
/** \endcond public */
/** \cond public */

/**
 * @brief Push cdata of given \a ctypeid onto the stack.
 * CTypeID must be used from FFI at least once. Allocated memory returned
 * uninitialized. Only numbers and pointers are supported.
 * @param L Lua State
 * @param ctypeid FFI's CTypeID of this cdata
 * @sa luaL_checkcdata
 * @return memory associated with this cdata
 */
LUA_API void *
luaL_pushcdata(struct lua_State *L, uint32_t ctypeid);

/**
 * @brief Checks whether the function argument idx is a cdata
 * @param L Lua State
 * @param idx stack index
 * @param ctypeid FFI's CTypeID of this cdata
 * @sa luaL_pushcdata
 * @return memory associated with this cdata
 */
LUA_API void *
luaL_checkcdata(struct lua_State *L, int idx, uint32_t *ctypeid);

/**
 * @brief Sets finalizer function on a cdata object.
 * Equivalent to call ffi.gc(obj, function).
 * Finalizer function must be on the top of the stack.
 * @param L Lua State
 * @param idx object
 */
LUA_API void
luaL_setcdatagc(struct lua_State *L, int idx);

/**
* @brief Return CTypeID (FFI) of given СDATA type
* @param L Lua State
* @param ctypename С type name as string (e.g. "struct request" or "uint32_t")
* @sa luaL_pushcdata
* @sa luaL_checkcdata
* @return CTypeID
*/
LUA_API uint32_t
luaL_ctypeid(struct lua_State *L, const char *ctypename);

/**
* @brief Declare symbols for FFI
* @param L Lua State
* @param ctypename C definitions, e.g "struct stat"
* @sa ffi.cdef(def)
* @retval 0 on success
* @retval LUA_ERRRUN, LUA_ERRMEM, LUA_ERRERR otherwise
*/
LUA_API int
luaL_cdef(struct lua_State *L, const char *ctypename);

/** \endcond public */
/** \cond public */

/**
 * Push uint64_t onto the stack
 *
 * @param L is a Lua State
 * @param val is a value to push
 */
LUA_API void
luaL_pushuint64(struct lua_State *L, uint64_t val);

/**
 * Push int64_t onto the stack
 *
 * @param L is a Lua State
 * @param val is a value to push
 */
LUA_API void
luaL_pushint64(struct lua_State *L, int64_t val);

/**
 * Checks whether the argument idx is a uint64 or a convertable string and
 * returns this number.
 * \throws error if the argument can't be converted.
 */
LUA_API uint64_t
luaL_checkuint64(struct lua_State *L, int idx);

/**
 * Checks whether the argument idx is a int64 or a convertable string and
 * returns this number.
 * \throws error if the argument can't be converted.
 */
LUA_API int64_t
luaL_checkint64(struct lua_State *L, int idx);

/**
 * Checks whether the argument idx is a uint64 or a convertable string and
 * returns this number.
 * \return the converted number or 0 of argument can't be converted.
 */
LUA_API uint64_t
luaL_touint64(struct lua_State *L, int idx);

/**
 * Checks whether the argument idx is a int64 or a convertable string and
 * returns this number.
 * \return the converted number or 0 of argument can't be converted.
 */
LUA_API int64_t
luaL_toint64(struct lua_State *L, int idx);

/**
 * Like lua_call(), but with the proper support of Tarantool errors.
 * \sa lua_call()
 */
LUA_API int
luaT_call(lua_State *L, int nargs, int nreturns);

/**
 * Like lua_cpcall(), but with the proper support of Tarantool errors.
 * \sa lua_cpcall()
 */
LUA_API int
luaT_cpcall(lua_State *L, lua_CFunction func, void *ud);

/**
 * Get global Lua state used by Tarantool
 */
LUA_API lua_State *
luaT_state(void);

/**
 * Like lua_tolstring, but supports metatables, booleans and nil properly.
 */
LUA_API const char *
luaT_tolstring(lua_State *L, int idx, size_t *ssize);

/**
 * Check whether a Lua object is a function or has
 * metatable/metatype with a __call field.
 *
 * Note: It does not check type of __call metatable/metatype
 * field.
 */
LUA_API int
luaL_iscallable(lua_State *L, int idx);

/** \endcond public */
/** \cond public */
struct error;

/**
 * Re-throws the last Tarantool error as a Lua object.
 * \sa lua_error()
 * \sa box_error_last()
 */
LUA_API int
luaT_error(lua_State *L);

/**
 * Return nil as the first return value and an error as the
 * second. The error is received using box_error_last().
 *
 * @param L Lua stack.
 */
LUA_API int
luaT_push_nil_and_error(lua_State *L);

void
luaT_pusherror(struct lua_State *L, struct error *e);
/** \endcond public */
/** \cond public */

/**
 * Transaction id - a non-persistent unique identifier
 * of the current transaction. -1 if there is no current
 * transaction.
 */
API_EXPORT int64_t
box_txn_id(void);

/**
 * Return true if there is an active transaction.
 */
API_EXPORT bool
box_txn(void);

/**
 * Begin a transaction in the current fiber.
 *
 * A transaction is attached to caller fiber, therefore one fiber can have
 * only one active transaction.
 *
 * @retval 0 - success
 * @retval -1 - failed, perhaps a transaction has already been
 * started
 */
API_EXPORT int
box_txn_begin(void);

/**
 * Commit the current transaction.
 * @retval 0 - success
 * @retval -1 - failed, perhaps a disk write failure.
 * started
 */
API_EXPORT int
box_txn_commit(void);

/**
 * Rollback the current transaction.
 * May fail if called from a nested
 * statement.
 */
API_EXPORT int
box_txn_rollback(void);

/**
 * Allocate memory on txn memory pool.
 * The memory is automatically deallocated when the transaction
 * is committed or rolled back.
 *
 * @retval NULL out of memory
 */
API_EXPORT void *
box_txn_alloc(size_t size);

/** \endcond public */
/** \cond public */

typedef struct key_def box_key_def_t;
typedef struct tuple box_tuple_t;

/**
 * Create key definition with key fields with passed typed on passed positions.
 * May be used for tuple format creation and/or tuple comparison.
 *
 * \param fields array with key field identifiers
 * \param types array with key field types (see enum field_type)
 * \param part_count the number of key fields
 * \returns a new key definition object
 */
box_key_def_t *
box_key_def_new(uint32_t *fields, uint32_t *types, uint32_t part_count);

/**
 * Delete key definition
 *
 * \param key_def key definition to delete
 */
void
box_key_def_delete(box_key_def_t *key_def);

/**
 * Compare tuples using the key definition.
 * @param tuple_a first tuple
 * @param tuple_b second tuple
 * @param key_def key definition
 * @retval 0  if key_fields(tuple_a) == key_fields(tuple_b)
 * @retval <0 if key_fields(tuple_a) < key_fields(tuple_b)
 * @retval >0 if key_fields(tuple_a) > key_fields(tuple_b)
 */
int
box_tuple_compare(box_tuple_t *tuple_a, box_tuple_t *tuple_b,
		  box_key_def_t *key_def);

/**
 * @brief Compare tuple with key using the key definition.
 * @param tuple tuple
 * @param key key with MessagePack array header
 * @param key_def key definition
 *
 * @retval 0  if key_fields(tuple) == parts(key)
 * @retval <0 if key_fields(tuple) < parts(key)
 * @retval >0 if key_fields(tuple) > parts(key)
 */

int
box_tuple_compare_with_key(box_tuple_t *tuple_a, const char *key_b,
			   box_key_def_t *key_def);

/** \endcond public */
/** \cond public */

/*
 * Possible field data types. Can't use STRS/ENUM macros for them,
 * since there is a mismatch between enum name (STRING) and type
 * name literal ("STR"). STR is already used as Objective C type.
 */
enum field_type {
	FIELD_TYPE_ANY = 0,
	FIELD_TYPE_UNSIGNED,
	FIELD_TYPE_STRING,
	FIELD_TYPE_NUMBER,
	FIELD_TYPE_DOUBLE,
	FIELD_TYPE_INTEGER,
	FIELD_TYPE_BOOLEAN,
	FIELD_TYPE_VARBINARY,
	FIELD_TYPE_SCALAR,
	FIELD_TYPE_DECIMAL,
	FIELD_TYPE_UUID,
	FIELD_TYPE_ARRAY,
	FIELD_TYPE_MAP,
	field_type_MAX
};

enum on_conflict_action {
	ON_CONFLICT_ACTION_NONE = 0,
	ON_CONFLICT_ACTION_ROLLBACK,
	ON_CONFLICT_ACTION_ABORT,
	ON_CONFLICT_ACTION_FAIL,
	ON_CONFLICT_ACTION_IGNORE,
	ON_CONFLICT_ACTION_REPLACE,
	ON_CONFLICT_ACTION_DEFAULT,
	on_conflict_action_MAX
};

/** \endcond public */
/** \cond public */

typedef struct tuple_format box_tuple_format_t;

/**
 * Tuple Format.
 *
 * Each Tuple has associated format (class). Default format is used to
 * create tuples which are not attach to any particular space.
 */
box_tuple_format_t *
box_tuple_format_default(void);

/**
 * Tuple
 */
typedef struct tuple box_tuple_t;

/**
 * Increase the reference counter of tuple.
 *
 * Tuples are reference counted. All functions that return tuples guarantee
 * that the last returned tuple is refcounted internally until the next
 * call to API function that yields or returns another tuple.
 *
 * You should increase the reference counter before taking tuples for long
 * processing in your code. Such tuples will not be garbage collected even
 * if another fiber remove they from space. After processing please
 * decrement the reference counter using box_tuple_unref(), otherwise the
 * tuple will leak.
 *
 * \param tuple a tuple
 * \retval 0 always
 * \sa box_tuple_unref()
 */
int
box_tuple_ref(box_tuple_t *tuple);

/**
 * Decrease the reference counter of tuple.
 *
 * \param tuple a tuple
 * \sa box_tuple_ref()
 */
void
box_tuple_unref(box_tuple_t *tuple);

/**
 * Return the number of fields in tuple (the size of MsgPack Array).
 * \param tuple a tuple
 */
uint32_t
box_tuple_field_count(box_tuple_t *tuple);

/**
 * Return the number of bytes used to store internal tuple data (MsgPack Array).
 * \param tuple a tuple
 */
size_t
box_tuple_bsize(box_tuple_t *tuple);

/**
 * Dump raw MsgPack data to the memory byffer \a buf of size \a size.
 *
 * Store tuple fields in the memory buffer.
 * \retval -1 on error.
 * \retval number of bytes written on success.
 * Upon successful return, the function returns the number of bytes written.
 * If buffer size is not enough then the return value is the number of bytes
 * which would have been written if enough space had been available.
 */
ssize_t
box_tuple_to_buf(box_tuple_t *tuple, char *buf, size_t size);

/**
 * Return the associated format.
 * \param tuple tuple
 * \return tuple_format
 */
box_tuple_format_t *
box_tuple_format(box_tuple_t *tuple);

/**
 * Return the raw tuple field in MsgPack format.
 *
 * The buffer is valid until next call to box_tuple_* functions.
 *
 * \param tuple a tuple
 * \param fieldno zero-based index in MsgPack array.
 * \retval NULL if i >= box_tuple_field_count(tuple)
 * \retval msgpack otherwise
 */
const char *
box_tuple_field(box_tuple_t *tuple, uint32_t fieldno);

/**
 * Tuple iterator
 */
typedef struct tuple_iterator box_tuple_iterator_t;

/**
 * Allocate and initialize a new tuple iterator. The tuple iterator
 * allow to iterate over fields at root level of MsgPack array.
 *
 * Example:
 * \code
 * box_tuple_iterator *it = box_tuple_iterator(tuple);
 * if (it == NULL) {
 *      // error handling using box_error_last()
 * }
 * const char *field;
 * while (field = box_tuple_next(it)) {
 *      // process raw MsgPack data
 * }
 *
 * // rewind iterator to first position
 * box_tuple_rewind(it);
 * assert(box_tuple_position(it) == 0);
 *
 * // rewind iterator to first position
 * field = box_tuple_seek(it, 3);
 * assert(box_tuple_position(it) == 4);
 *
 * box_iterator_free(it);
 * \endcode
 *
 * \post box_tuple_position(it) == 0
 */
box_tuple_iterator_t *
box_tuple_iterator(box_tuple_t *tuple);

/**
 * Destroy and free tuple iterator
 */
void
box_tuple_iterator_free(box_tuple_iterator_t *it);

/**
 * Return zero-based next position in iterator.
 * That is, this function return the field id of field that will be
 * returned by the next call to box_tuple_next(it). Returned value is zero
 * after initialization or rewind and box_tuple_field_count(tuple)
 * after the end of iteration.
 *
 * \param it tuple iterator
 * \returns position.
 */
uint32_t
box_tuple_position(box_tuple_iterator_t *it);

/**
 * Rewind iterator to the initial position.
 *
 * \param it tuple iterator
 * \post box_tuple_position(it) == 0
 */
void
box_tuple_rewind(box_tuple_iterator_t *it);

/**
 * Seek the tuple iterator.
 *
 * The returned buffer is valid until next call to box_tuple_* API.
 * Requested fieldno returned by next call to box_tuple_next(it).
 *
 * \param it tuple iterator
 * \param fieldno - zero-based position in MsgPack array.
 * \post box_tuple_position(it) == fieldno if returned value is not NULL
 * \post box_tuple_position(it) == box_tuple_field_count(tuple) if returned
 * value is NULL.
 */
const char *
box_tuple_seek(box_tuple_iterator_t *it, uint32_t fieldno);

/**
 * Return the next tuple field from tuple iterator.
 * The returned buffer is valid until next call to box_tuple_* API.
 *
 * \param it tuple iterator.
 * \retval NULL if there are no more fields.
 * \retval MsgPack otherwise
 * \pre box_tuple_position(it) is zerod-based id of returned field
 * \post box_tuple_position(it) == box_tuple_field_count(tuple) if returned
 * value is NULL.
 */
const char *
box_tuple_next(box_tuple_iterator_t *it);

/**
 * Allocate and initialize a new tuple from a raw MsgPack Array data.
 *
 * \param format tuple format.
 * Use box_tuple_format_default() to create space-independent tuple.
 * \param data tuple data in MsgPack Array format ([field1, field2, ...]).
 * \param end the end of \a data
 * \retval tuple
 * \pre data, end is valid MsgPack Array
 * \sa \code box.tuple.new(data) \endcode
 */
box_tuple_t *
box_tuple_new(box_tuple_format_t *format, const char *data, const char *end);

box_tuple_t *
box_tuple_update(box_tuple_t *tuple, const char *expr, const char *expr_end);

box_tuple_t *
box_tuple_upsert(box_tuple_t *tuple, const char *expr, const char *expr_end);

/** \endcond public */
/** \cond public */

/**
 * Return new in-memory tuple format based on passed key definitions.
 *
 * \param keys array of keys defined for the format
 * \key_count count of keys
 * \retval new tuple format if success
 * \retval NULL for error
 */
box_tuple_format_t *
box_tuple_format_new(struct key_def **keys, uint16_t key_count);

/**
 * Increment tuple format ref count.
 *
 * \param tuple_format the tuple format to ref
 */
void
box_tuple_format_ref(box_tuple_format_t *format);

/**
 * Decrement tuple format ref count.
 *
 * \param tuple_format the tuple format to unref
 */
void
box_tuple_format_unref(box_tuple_format_t *format);

/** \endcond public */
/** \cond public */
enum {
	/** Start of the reserved range of system spaces. */
	BOX_SYSTEM_ID_MIN = 256,
	/** Space if of _vinyl_deferred_delete. */
	BOX_VINYL_DEFERRED_DELETE_ID = 257,
	/** Space id of _schema. */
	BOX_SCHEMA_ID = 272,
	/** Space id of _collation. */
	BOX_COLLATION_ID = 276,
	/** Space id of _vcollation. */
	BOX_VCOLLATION_ID = 277,
	/** Space id of _space. */
	BOX_SPACE_ID = 280,
	/** Space id of _vspace view. */
	BOX_VSPACE_ID = 281,
	/** Space id of _sequence. */
	BOX_SEQUENCE_ID = 284,
	/** Space id of _sequence_data. */
	BOX_SEQUENCE_DATA_ID = 285,
	/** Space id of _vsequence view. */
	BOX_VSEQUENCE_ID = 286,
	/** Space id of _index. */
	BOX_INDEX_ID = 288,
	/** Space id of _vindex view. */
	BOX_VINDEX_ID = 289,
	/** Space id of _func. */
	BOX_FUNC_ID = 296,
	/** Space id of _vfunc view. */
	BOX_VFUNC_ID = 297,
	/** Space id of _user. */
	BOX_USER_ID = 304,
	/** Space id of _vuser view. */
	BOX_VUSER_ID = 305,
	/** Space id of _priv. */
	BOX_PRIV_ID = 312,
	/** Space id of _vpriv view. */
	BOX_VPRIV_ID = 313,
	/** Space id of _cluster. */
	BOX_CLUSTER_ID = 320,
	/** Space id of _trigger. */
	BOX_TRIGGER_ID = 328,
	/** Space id of _truncate. */
	BOX_TRUNCATE_ID = 330,
	/** Space id of _space_sequence. */
	BOX_SPACE_SEQUENCE_ID = 340,
	/** Space id of _fk_constraint. */
	BOX_FK_CONSTRAINT_ID = 356,
	/** Space id of _ck_contraint. */
	BOX_CK_CONSTRAINT_ID = 364,
	/** Space id of _func_index. */
	BOX_FUNC_INDEX_ID = 372,
	/** Space id of _session_settings. */
	BOX_SESSION_SETTINGS_ID = 380,
	/** End of the reserved range of system spaces. */
	BOX_SYSTEM_ID_MAX = 511,
	BOX_ID_NIL = 2147483647
};
/** \endcond public */
/** \cond public */

/*
 * Opaque structure passed to the stored C procedure
 */
typedef struct box_function_ctx box_function_ctx_t;

/**
 * Return a tuple from stored C procedure.
 *
 * Returned tuple is automatically reference counted by Tarantool.
 *
 * \param ctx an opaque structure passed to the stored C procedure by
 * Tarantool
 * \param tuple a tuple to return
 * \retval -1 on error (perhaps, out of memory; check box_error_last())
 * \retval 0 otherwise
 */
API_EXPORT int
box_return_tuple(box_function_ctx_t *ctx, box_tuple_t *tuple);

/**
 * Return MessagePack from a stored C procedure. The MessagePack
 * is copied, so it is safe to free/reuse the passed arguments
 * after the call.
 * MessagePack is not validated, for the sake of speed. It is
 * expected to be a single encoded object. An attempt to encode
 * and return multiple objects without wrapping them into an
 * MP_ARRAY or MP_MAP is undefined behaviour.
 *
 * \param ctx An opaque structure passed to the stored C procedure
 *        by Tarantool.
 * \param mp Begin of MessagePack.
 * \param mp_end End of MessagePack.
 * \retval -1 Error.
 * \retval 0 Success.
 */
API_EXPORT int
box_return_mp(box_function_ctx_t *ctx, const char *mp, const char *mp_end);

/**
 * Find space id by name.
 *
 * This function performs SELECT request to _vspace system space.
 * \param name space name
 * \param len length of \a name
 * \retval BOX_ID_NIL on error or if not found (check box_error_last())
 * \retval space_id otherwise
 * \sa box_index_id_by_name
 */
API_EXPORT uint32_t
box_space_id_by_name(const char *name, uint32_t len);

/**
 * Find index id by name.
 *
 * This function performs SELECT request to _vindex system space.
 * \param space_id space identifier
 * \param name index name
 * \param len length of \a name
 * \retval BOX_ID_NIL on error or if not found (check box_error_last())
 * \retval index_id otherwise
 * \sa box_space_id_by_name
 */
API_EXPORT uint32_t
box_index_id_by_name(uint32_t space_id, const char *name, uint32_t len);

/**
 * Execute an INSERT request.
 *
 * \param space_id space identifier
 * \param tuple encoded tuple in MsgPack Array format ([ field1, field2, ...])
 * \param tuple_end end of @a tuple
 * \param[out] result a new tuple. Can be set to NULL to discard result.
 * \retval -1 on error (check box_error_last())
 * \retval 0 on success
 * \sa \code box.space[space_id]:insert(tuple) \endcode
 */
API_EXPORT int
box_insert(uint32_t space_id, const char *tuple, const char *tuple_end,
	   box_tuple_t **result);

/**
 * Execute an REPLACE request.
 *
 * \param space_id space identifier
 * \param tuple encoded tuple in MsgPack Array format ([ field1, field2, ...])
 * \param tuple_end end of @a tuple
 * \param[out] result a new tuple. Can be set to NULL to discard result.
 * \retval -1 on error (check box_error_last())
 * \retval 0 on success
 * \sa \code box.space[space_id]:replace(tuple) \endcode
 */
API_EXPORT int
box_replace(uint32_t space_id, const char *tuple, const char *tuple_end,
	    box_tuple_t **result);

/**
 * Execute an DELETE request.
 *
 * \param space_id space identifier
 * \param index_id index identifier
 * \param key encoded key in MsgPack Array format ([part1, part2, ...]).
 * \param key_end the end of encoded \a key.
 * \param[out] result an old tuple. Can be set to NULL to discard result.
 * \retval -1 on error (check box_error_last())
 * \retval 0 on success
 * \sa \code box.space[space_id].index[index_id]:delete(key) \endcode
 */
API_EXPORT int
box_delete(uint32_t space_id, uint32_t index_id, const char *key,
	   const char *key_end, box_tuple_t **result);

/**
 * Execute an UPDATE request.
 *
 * \param space_id space identifier
 * \param index_id index identifier
 * \param key encoded key in MsgPack Array format ([part1, part2, ...]).
 * \param key_end the end of encoded \a key.
 * \param ops encoded operations in MsgPack Arrat format, e.g.
 * [ [ '=', fieldno,  value ],  ['!', 2, 'xxx'] ]
 * \param ops_end the end of encoded \a ops
 * \param index_base 0 if fieldnos in update operations are zero-based
 * indexed (like C) or 1 if for one-based indexed field ids (like Lua).
 * \param[out] result a new tuple. Can be set to NULL to discard result.
 * \retval -1 on error (check box_error_last())
 * \retval 0 on success
 * \sa \code box.space[space_id].index[index_id]:update(key, ops) \endcode
 * \sa box_upsert()
 */
API_EXPORT int
box_update(uint32_t space_id, uint32_t index_id, const char *key,
	   const char *key_end, const char *ops, const char *ops_end,
	   int index_base, box_tuple_t **result);

/**
 * Execute an UPSERT request.
 *
 * \param space_id space identifier
 * \param index_id index identifier
 * \param ops encoded operations in MsgPack Arrat format, e.g.
 * [ [ '=', fieldno,  value ],  ['!', 2, 'xxx'] ]
 * \param ops_end the end of encoded \a ops
 * \param tuple encoded tuple in MsgPack Array format ([ field1, field2, ...])
 * \param tuple_end end of @a tuple
 * \param index_base 0 if fieldnos in update operations are zero-based
 * indexed (like C) or 1 if for one-based indexed field ids (like Lua).
 * \param[out] result a new tuple. Can be set to NULL to discard result.
 * \retval -1 on error (check box_error_last())
 * \retval 0 on success
 * \sa \code box.space[space_id].index[index_id]:update(key, ops) \endcode
 * \sa box_update()
 */
API_EXPORT int
box_upsert(uint32_t space_id, uint32_t index_id, const char *tuple,
	   const char *tuple_end, const char *ops, const char *ops_end,
	   int index_base, box_tuple_t **result);

/**
 * Truncate space.
 *
 * \param space_id space identifier
 */
API_EXPORT int
box_truncate(uint32_t space_id);

/**
 * Advance a sequence.
 *
 * \param seq_id sequence identifier
 * \param[out] result pointer to a variable where the next sequence
 * value will be stored on success
 * \retval -1 on error (check box_error_last())
 * \retval 0 on success
 */
API_EXPORT int
box_sequence_next(uint32_t seq_id, int64_t *result);

/**
 * Get the last value returned by a sequence.
 *
 * \param seq_id sequence identifier
 * \param[out] result pointer to a variable where the current sequence
 * value will be stored on success
 * \retval -1 on error (check box_error_last())
 * \retval 0 on success
 */
API_EXPORT int
box_sequence_current(uint32_t seq_id, int64_t *result);

/**
 * Set a sequence value.
 *
 * \param seq_id sequence identifier
 * \param value new sequence value; on success the next call to
 * box_sequence_next() will return the value following \a value
 * \retval -1 on error (check box_error_last())
 * \retval 0 on success
 */
API_EXPORT int
box_sequence_set(uint32_t seq_id, int64_t value);

/**
 * Reset a sequence.
 *
 * \param seq_id sequence identifier
 * \retval -1 on error (check box_error_last())
 * \retval 0 on success
 */
API_EXPORT int
box_sequence_reset(uint32_t seq_id);

/**
 * Push MessagePack data into a session data channel - socket,
 * console or whatever is behind the session. Note, that
 * successful push does not guarantee delivery in case it was sent
 * into the network. Just like with write()/send() system calls.
 *
 * \param data begin of MessagePack to push
 * \param data_end end of MessagePack to push
 * \retval -1 on error (check box_error_last())
 * \retval 0 on success
 */
API_EXPORT int
box_session_push(const char *data, const char *data_end);

/** \endcond public */
/** \cond public */

typedef struct tuple box_tuple_t;
typedef struct key_def box_key_def_t;
typedef struct iterator box_iterator_t;

/**
 * Allocate and initialize iterator for space_id, index_id.
 *
 * A returned iterator must be destroyed by box_iterator_free().
 *
 * \param space_id space identifier.
 * \param index_id index identifier.
 * \param type \link iterator_type iterator type \endlink
 * \param key encoded key in MsgPack Array format ([part1, part2, ...]).
 * \param key_end the end of encoded \a key
 * \retval NULL on error (check box_error_last())
 * \retval iterator otherwise
 * \sa box_iterator_next()
 * \sa box_iterator_free()
 */
box_iterator_t *
box_index_iterator(uint32_t space_id, uint32_t index_id, int type,
		   const char *key, const char *key_end);
/**
 * Retrive the next item from the \a iterator.
 *
 * \param iterator an iterator returned by box_index_iterator().
 * \param[out] result a tuple or NULL if there is no more data.
 * \retval -1 on error (check box_error_last() for details)
 * \retval 0 on success. The end of data is not an error.
 */
int
box_iterator_next(box_iterator_t *iterator, box_tuple_t **result);

/**
 * Destroy and deallocate iterator.
 *
 * \param iterator an interator returned by box_index_iterator()
 */
void
box_iterator_free(box_iterator_t *iterator);

/**
 * Return the number of element in the index.
 *
 * \param space_id space identifier
 * \param index_id index identifier
 * \retval -1 on error (check box_error_last())
 * \retval >= 0 otherwise
 */
ssize_t
box_index_len(uint32_t space_id, uint32_t index_id);

/**
 * Return the number of bytes used in memory by the index.
 *
 * \param space_id space identifier
 * \param index_id index identifier
 * \retval -1 on error (check box_error_last())
 * \retval >= 0 otherwise
 */
ssize_t
box_index_bsize(uint32_t space_id, uint32_t index_id);

/**
 * Return a random tuple from the index (useful for statistical analysis).
 *
 * \param space_id space identifier
 * \param index_id index identifier
 * \param rnd random seed
 * \param[out] result a tuple or NULL if index is empty
 * \retval -1 on error (check box_error_last())
 * \retval 0 on success
 * \sa \code box.space[space_id].index[index_id]:random(rnd) \endcode
 */
int
box_index_random(uint32_t space_id, uint32_t index_id, uint32_t rnd,
		box_tuple_t **result);

/**
 * Get a tuple from index by the key.
 *
 * Please note that this function works much more faster than
 * box_select() or box_index_iterator() + box_iterator_next().
 *
 * \param space_id space identifier
 * \param index_id index identifier
 * \param key encoded key in MsgPack Array format ([part1, part2, ...]).
 * \param key_end the end of encoded \a key
 * \param[out] result a tuple or NULL if index is empty
 * \retval -1 on error (check box_error_last())
 * \retval 0 on success
 * \pre key != NULL
 * \sa \code box.space[space_id].index[index_id]:get(key) \endcode
 */
int
box_index_get(uint32_t space_id, uint32_t index_id, const char *key,
	      const char *key_end, box_tuple_t **result);

/**
 * Return a first (minimal) tuple matched the provided key.
 *
 * \param space_id space identifier
 * \param index_id index identifier
 * \param key encoded key in MsgPack Array format ([part1, part2, ...]).
 * \param key_end the end of encoded \a key.
 * \param[out] result a tuple or NULL if index is empty
 * \retval -1 on error (check box_error_last())
 * \retval 0 on success
 * \sa \code box.space[space_id].index[index_id]:min(key) \endcode
 */
int
box_index_min(uint32_t space_id, uint32_t index_id, const char *key,
	      const char *key_end, box_tuple_t **result);

/**
 * Return a last (maximal) tuple matched the provided key.
 *
 * \param space_id space identifier
 * \param index_id index identifier
 * \param key encoded key in MsgPack Array format ([part1, part2, ...]).
 * \param key_end the end of encoded \a key.
 * \param[out] result a tuple or NULL if index is empty
 * \retval -1 on error (check box_error_last())
 * \retval 0 on success
 * \sa \code box.space[space_id].index[index_id]:max(key) \endcode
 */
int
box_index_max(uint32_t space_id, uint32_t index_id, const char *key,
	      const char *key_end, box_tuple_t **result);

/**
 * Count the number of tuple matched the provided key.
 *
 * \param space_id space identifier
 * \param index_id index identifier
 * \param type iterator type - enum \link iterator_type \endlink
 * \param key encoded key in MsgPack Array format ([part1, part2, ...]).
 * \param key_end the end of encoded \a key.
 * \retval -1 on error (check box_error_last())
 * \retval >=0 on success
 * \sa \code box.space[space_id].index[index_id]:count(key,
 *     { iterator = type }) \endcode
 */
ssize_t
box_index_count(uint32_t space_id, uint32_t index_id, int type,
		const char *key, const char *key_end);

/**
 * Extract key from tuple according to key definition of given
 * index. Returned buffer is allocated on box_txn_alloc() with
 * this key.
 * @param tuple Tuple from which need to extract key.
 * @param space_id Space identifier.
 * @param index_id Index identifier.
 * @retval not NULL Success
 * @retval     NULL Memory Allocation error
 */
char *
box_tuple_extract_key(box_tuple_t *tuple, uint32_t space_id,
		      uint32_t index_id, uint32_t *key_size);

/** \endcond public */
/** \cond public */

/**
 * Controls how to iterate over tuples in an index.
 * Different index types support different iterator types.
 * For example, one can start iteration from a particular value
 * (request key) and then retrieve all tuples where keys are
 * greater or equal (= GE) to this key.
 *
 * If iterator type is not supported by the selected index type,
 * iterator constructor must fail with ER_UNSUPPORTED. To be
 * selectable for primary key, an index must support at least
 * ITER_EQ and ITER_GE types.
 *
 * NULL value of request key corresponds to the first or last
 * key in the index, depending on iteration direction.
 * (first key for GE and GT types, and last key for LE and LT).
 * Therefore, to iterate over all tuples in an index, one can
 * use ITER_GE or ITER_LE iteration types with start key equal
 * to NULL.
 * For ITER_EQ, the key must not be NULL.
 */
enum iterator_type {
	/* ITER_EQ must be the first member for request_create  */
	ITER_EQ               =  0, /* key == x ASC order                  */
	ITER_REQ              =  1, /* key == x DESC order                 */
	ITER_ALL              =  2, /* all tuples                          */
	ITER_LT               =  3, /* key <  x                            */
	ITER_LE               =  4, /* key <= x                            */
	ITER_GE               =  5, /* key >= x                            */
	ITER_GT               =  6, /* key >  x                            */
	ITER_BITS_ALL_SET     =  7, /* all bits from x are set in key      */
	ITER_BITS_ANY_SET     =  8, /* at least one x's bit is set         */
	ITER_BITS_ALL_NOT_SET =  9, /* all bits are not set                */
	ITER_OVERLAPS         = 10, /* key overlaps x                      */
	ITER_NEIGHBOR         = 11, /* tuples in distance ascending order from specified point */
	iterator_type_MAX
};

/** \endcond public */
/** \cond public */

struct error;
/**
 * Error - contains information about error.
 */
typedef struct error box_error_t;

/**
 * Return the error type, e.g. "ClientError", "SocketError", etc.
 * \param error
 * \return not-null string
 */
const char *
box_error_type(const box_error_t *error);

/**
 * Return IPROTO error code
 * \param error error
 * \return enum box_error_code
 */
uint32_t
box_error_code(const box_error_t *error);

/**
 * Return the error message
 * \param error error
 * \return not-null string
 */
const char *
box_error_message(const box_error_t *error);

/**
 * Get the information about the last API call error.
 *
 * The Tarantool error handling works most like libc's errno. All API calls
 * return -1 or NULL in the event of error. An internal pointer to
 * box_error_t type is set by API functions to indicate what went wrong.
 * This value is only significant if API call failed (returned -1 or NULL).
 *
 * Successful function can also touch the last error in some
 * cases. You don't have to clear the last error before calling
 * API functions. The returned object is valid only until next
 * call to **any** API function.
 *
 * You must set the last error using box_error_set() in your stored C
 * procedures if you want to return a custom error message.
 * You can re-throw the last API error to IPROTO client by keeping
 * the current value and returning -1 to Tarantool from your
 * stored procedure.
 *
 * \return last error.
 */
box_error_t *
box_error_last(void);

/**
 * Clear the last error.
 */
void
box_error_clear(void);

/**
 * Set the last error.
 *
 * \param code IPROTO error code (enum \link box_error_code \endlink)
 * \param format (const char * ) - printf()-like format string
 * \param ... - format arguments
 * \returns -1 for convention use
 *
 * \sa enum box_error_code
 */
int
box_error_set(const char *file, unsigned line, uint32_t code,
	      const char *format, ...);

/**
 * A backward-compatible API define.
 */
#define box_error_raise(code, format, ...) \
	box_error_set(__FILE__, __LINE__, code, format, ##__VA_ARGS__)

/** \endcond public */
/** \cond public */

/**
 * Checks whether the argument idx is a tuple and
 * returns it.
 *
 * @param L Lua State
 * @param idx the stack index
 * @retval non-NULL argument is tuple
 * @throws error if the argument is not a tuple.
 */
box_tuple_t *
luaT_checktuple(struct lua_State *L, int idx);

/**
 * Push a tuple onto the stack.
 * @param L Lua State
 * @sa luaT_istuple
 * @throws on OOM
 */
void
luaT_pushtuple(struct lua_State *L, box_tuple_t *tuple);

/**
 * Checks whether argument idx is a tuple.
 *
 * @param L Lua State
 * @param idx the stack index
 * @retval non-NULL argument is tuple
 * @retval NULL argument is not tuple
 */
box_tuple_t *
luaT_istuple(struct lua_State *L, int idx);

/** \endcond public */
/** \cond public */

/**
 * A lock for cooperative multitasking environment
 */
typedef struct box_latch box_latch_t;

/**
 * Allocate and initialize the new latch.
 * \returns latch
 */
box_latch_t*
box_latch_new(void);

/**
 * Destroy and free the latch.
 * \param latch latch
 */
void
box_latch_delete(box_latch_t *latch);

/**
* Lock a latch. Waits indefinitely until the current fiber can gain access to
* the latch.
*
* \param latch a latch
*/
void
box_latch_lock(box_latch_t *latch);

/**
 * Try to lock a latch. Return immediately if the latch is locked.
 * \param latch a latch
 * \retval 0 - success
 * \retval 1 - the latch is locked.
 */
int
box_latch_trylock(box_latch_t *latch);

/**
 * Unlock a latch. The fiber calling this function must
 * own the latch.
 *
 * \param latch a latch
 */
void
box_latch_unlock(box_latch_t *latch);

/** \endcond public */
/** \cond public */

double clock_realtime(void);
double clock_monotonic(void);
double clock_process(void);
double clock_thread(void);

uint64_t clock_realtime64(void);
uint64_t clock_monotonic64(void);
uint64_t clock_process64(void);
uint64_t clock_thread64(void);

/** \endcond public */
enum box_error_code { ER_UNKNOWN, ER_ILLEGAL_PARAMS, ER_MEMORY_ISSUE, ER_TUPLE_FOUND, ER_TUPLE_NOT_FOUND, ER_UNSUPPORTED, ER_NONMASTER, ER_READONLY, ER_INJECTION, ER_CREATE_SPACE, ER_SPACE_EXISTS, ER_DROP_SPACE, ER_ALTER_SPACE, ER_INDEX_TYPE, ER_MODIFY_INDEX, ER_LAST_DROP, ER_TUPLE_FORMAT_LIMIT, ER_DROP_PRIMARY_KEY, ER_KEY_PART_TYPE, ER_EXACT_MATCH, ER_INVALID_MSGPACK, ER_PROC_RET, ER_TUPLE_NOT_ARRAY, ER_FIELD_TYPE, ER_INDEX_PART_TYPE_MISMATCH, ER_UPDATE_SPLICE, ER_UPDATE_ARG_TYPE, ER_FORMAT_MISMATCH_INDEX_PART, ER_UNKNOWN_UPDATE_OP, ER_UPDATE_FIELD, ER_FUNCTION_TX_ACTIVE, ER_KEY_PART_COUNT, ER_PROC_LUA, ER_NO_SUCH_PROC, ER_NO_SUCH_TRIGGER, ER_NO_SUCH_INDEX_ID, ER_NO_SUCH_SPACE, ER_NO_SUCH_FIELD_NO, ER_EXACT_FIELD_COUNT, ER_FIELD_MISSING, ER_WAL_IO, ER_MORE_THAN_ONE_TUPLE, ER_ACCESS_DENIED, ER_CREATE_USER, ER_DROP_USER, ER_NO_SUCH_USER, ER_USER_EXISTS, ER_PASSWORD_MISMATCH, ER_UNKNOWN_REQUEST_TYPE, ER_UNKNOWN_SCHEMA_OBJECT, ER_CREATE_FUNCTION, ER_NO_SUCH_FUNCTION, ER_FUNCTION_EXISTS, ER_BEFORE_REPLACE_RET, ER_MULTISTATEMENT_TRANSACTION, ER_TRIGGER_EXISTS, ER_USER_MAX, ER_NO_SUCH_ENGINE, ER_RELOAD_CFG, ER_CFG, ER_SAVEPOINT_EMPTY_TX, ER_NO_SUCH_SAVEPOINT, ER_UNKNOWN_REPLICA, ER_REPLICASET_UUID_MISMATCH, ER_INVALID_UUID, ER_REPLICASET_UUID_IS_RO, ER_INSTANCE_UUID_MISMATCH, ER_REPLICA_ID_IS_RESERVED, ER_INVALID_ORDER, ER_MISSING_REQUEST_FIELD, ER_IDENTIFIER, ER_DROP_FUNCTION, ER_ITERATOR_TYPE, ER_REPLICA_MAX, ER_INVALID_XLOG, ER_INVALID_XLOG_NAME, ER_INVALID_XLOG_ORDER, ER_NO_CONNECTION, ER_TIMEOUT, ER_ACTIVE_TRANSACTION, ER_CURSOR_NO_TRANSACTION, ER_CROSS_ENGINE_TRANSACTION, ER_NO_SUCH_ROLE, ER_ROLE_EXISTS, ER_CREATE_ROLE, ER_INDEX_EXISTS, ER_SESSION_CLOSED, ER_ROLE_LOOP, ER_GRANT, ER_PRIV_GRANTED, ER_ROLE_GRANTED, ER_PRIV_NOT_GRANTED, ER_ROLE_NOT_GRANTED, ER_MISSING_SNAPSHOT, ER_CANT_UPDATE_PRIMARY_KEY, ER_UPDATE_INTEGER_OVERFLOW, ER_GUEST_USER_PASSWORD, ER_TRANSACTION_CONFLICT, ER_UNSUPPORTED_PRIV, ER_LOAD_FUNCTION, ER_FUNCTION_LANGUAGE, ER_RTREE_RECT, ER_PROC_C, ER_UNKNOWN_RTREE_INDEX_DISTANCE_TYPE, ER_PROTOCOL, ER_UPSERT_UNIQUE_SECONDARY_KEY, ER_WRONG_INDEX_RECORD, ER_WRONG_INDEX_PARTS, ER_WRONG_INDEX_OPTIONS, ER_WRONG_SCHEMA_VERSION, ER_MEMTX_MAX_TUPLE_SIZE, ER_WRONG_SPACE_OPTIONS, ER_UNSUPPORTED_INDEX_FEATURE, ER_VIEW_IS_RO, ER_NO_TRANSACTION, ER_SYSTEM, ER_LOADING, ER_CONNECTION_TO_SELF, ER_KEY_PART_IS_TOO_LONG, ER_COMPRESSION, ER_CHECKPOINT_IN_PROGRESS, ER_SUB_STMT_MAX, ER_COMMIT_IN_SUB_STMT, ER_ROLLBACK_IN_SUB_STMT, ER_DECOMPRESSION, ER_INVALID_XLOG_TYPE, ER_ALREADY_RUNNING, ER_INDEX_FIELD_COUNT_LIMIT, ER_LOCAL_INSTANCE_ID_IS_READ_ONLY, ER_BACKUP_IN_PROGRESS, ER_READ_VIEW_ABORTED, ER_INVALID_INDEX_FILE, ER_INVALID_RUN_FILE, ER_INVALID_VYLOG_FILE, ER_CHECKPOINT_ROLLBACK, ER_VY_QUOTA_TIMEOUT, ER_PARTIAL_KEY, ER_TRUNCATE_SYSTEM_SPACE, ER_LOAD_MODULE, ER_VINYL_MAX_TUPLE_SIZE, ER_WRONG_DD_VERSION, ER_WRONG_SPACE_FORMAT, ER_CREATE_SEQUENCE, ER_ALTER_SEQUENCE, ER_DROP_SEQUENCE, ER_NO_SUCH_SEQUENCE, ER_SEQUENCE_EXISTS, ER_SEQUENCE_OVERFLOW, ER_NO_SUCH_INDEX_NAME, ER_SPACE_FIELD_IS_DUPLICATE, ER_CANT_CREATE_COLLATION, ER_WRONG_COLLATION_OPTIONS, ER_NULLABLE_PRIMARY, ER_NO_SUCH_FIELD_NAME_IN_SPACE, ER_TRANSACTION_YIELD, ER_NO_SUCH_GROUP, ER_SQL_BIND_VALUE, ER_SQL_BIND_TYPE, ER_SQL_BIND_PARAMETER_MAX, ER_SQL_EXECUTE, ER_UPDATE_DECIMAL_OVERFLOW, ER_SQL_BIND_NOT_FOUND, ER_ACTION_MISMATCH, ER_VIEW_MISSING_SQL, ER_FOREIGN_KEY_CONSTRAINT, ER_NO_SUCH_MODULE, ER_NO_SUCH_COLLATION, ER_CREATE_FK_CONSTRAINT, ER_DROP_FK_CONSTRAINT, ER_NO_SUCH_CONSTRAINT, ER_CONSTRAINT_EXISTS, ER_SQL_TYPE_MISMATCH, ER_ROWID_OVERFLOW, ER_DROP_COLLATION, ER_ILLEGAL_COLLATION_MIX, ER_SQL_NO_SUCH_PRAGMA, ER_SQL_CANT_RESOLVE_FIELD, ER_INDEX_EXISTS_IN_SPACE, ER_INCONSISTENT_TYPES, ER_SQL_SYNTAX_WITH_POS, ER_SQL_STACK_OVERFLOW, ER_SQL_SELECT_WILDCARD, ER_SQL_STATEMENT_EMPTY, ER_SQL_KEYWORD_IS_RESERVED, ER_SQL_SYNTAX_NEAR_TOKEN, ER_SQL_UNKNOWN_TOKEN, ER_SQL_PARSER_GENERIC, ER_SQL_ANALYZE_ARGUMENT, ER_SQL_COLUMN_COUNT_MAX, ER_HEX_LITERAL_MAX, ER_INT_LITERAL_MAX, ER_SQL_PARSER_LIMIT, ER_INDEX_DEF_UNSUPPORTED, ER_CK_DEF_UNSUPPORTED, ER_MULTIKEY_INDEX_MISMATCH, ER_CREATE_CK_CONSTRAINT, ER_CK_CONSTRAINT_FAILED, ER_SQL_COLUMN_COUNT, ER_FUNC_INDEX_FUNC, ER_FUNC_INDEX_FORMAT, ER_FUNC_INDEX_PARTS, ER_NO_SUCH_FIELD_NAME, ER_FUNC_WRONG_ARG_COUNT, ER_BOOTSTRAP_READONLY, ER_SQL_FUNC_WRONG_RET_COUNT, ER_FUNC_INVALID_RETURN_TYPE, ER_SQL_PARSER_GENERIC_WITH_POS, ER_REPLICA_NOT_ANON, ER_CANNOT_REGISTER, ER_SESSION_SETTING_INVALID_VALUE, ER_SQL_PREPARE, ER_WRONG_QUERY_ID, ER_SEQUENCE_NOT_STARTED, ER_NO_SUCH_SESSION_SETTING, box_error_code_MAX };
#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */

#endif /* TARANTOOL_MODULE_H_INCLUDED */
