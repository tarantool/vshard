Name: tarantool-vshard
Version: 0.1.0
Release: 1%{?dist}
Summary: The new generation of sharding based on virtual buckets
Group: Applications/Databases
License: BSD
URL: https://github.com/tarantool/vshard
Source0: https://github.com/tarantool/vshard/archive/%{version}/vshard-%{version}.tar.gz
BuildArch: noarch
BuildRequires: cmake >= 2.8
BuildRequires: gcc >= 4.5
BuildRequires: tarantool >= 1.9.0
BuildRequires: tarantool-devel >= 1.9.0
Requires: tarantool >= 1.9.0

# For tests
%if (0%{?fedora} >= 22 || 0%{?rhel} >= 7)
BuildRequires: python >= 2.7
BuildRequires: python-six >= 1.9.0
BuildRequires: python-gevent >= 1.0
BuildRequires: python-yaml >= 3.0.9
%endif

%description
The new generation of sharding based on virtual buckets.

%prep
%setup -q -n vshard-%{version}

%build
%cmake . -DCMAKE_BUILD_TYPE=RelWithDebInfo
make %{?_smp_mflags}

%check
%if (0%{?fedora} >= 22 || 0%{?rhel} >= 7)
make test
%endif

%install
%make_install

%files
# %{_libdir}/tarantool/vshard/
%{_datarootdir}/tarantool/vshard/
%doc README.md
%{!?_licensedir:%global license %doc}
%license LICENSE

%changelog
* Fri Dec 22 2017 Roman Tsisyk <roman@tarantool.org> 0.1.0-1
- Initial version
