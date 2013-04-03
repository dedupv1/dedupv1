
.PHONY: all clean install stylecheck dedupv1_core_test dedupv1d_test dedupv1d dedupv1_base_test contrib research

default:    all

all:
	./scripts/filter_path.py scons all

clean:
	scons -c | ./filter_path.py --stdin

install:
	./scripts/filter_path.py scons install

dedupv1_base_test:
	./scripts/filter_path.py scons dedupv1_base_test

dedupv1_core_test:
	./scripts/filter_path.py scons dedupv1_core_test

dedupv1_contrib_test:
	./scripts/filter_path.py scons dedupv1_contrib_test

dedupv1d_test:
	./scripts/filter_path.py scons dedupv1d_test
	
dedupv1d:
	./scripts/filter_path.py scons dedupv1d
	
contrib:
	./scripts/filter_path.py scons contrib

release:
	scons --release all | ./filter_path.py --stdin
		
release_clean:
	scons --release -c | ./filter_path.py --stdin

release_install:
	scons --release install | ./filter_path.py --stdin

valgrind:
	scons --no-tc-malloc --no-tbb-malloc --deadlock-avoidance-timeout=0 all | ./filter_path.py --stdin

valgrind_clean:
	scons -c | ./filter_path.py --stdin

valgrind_dedupv1_base_test:
	scons --no-tc-malloc --no-tbb-malloc --deadlock-avoidance-timeout=0 dedupv1_base_test | ./filter_path.py --stdin

valgrind_dedupv1_core_test:
	scons --no-tc-malloc --no-tbb-malloc --deadlock-avoidance-timeout=0 dedupv1_core_test | ./filter_path.py --stdin

valgrind_dedupv1_contrib_test:
	scons --no-tc-malloc --no-tbb-malloc --deadlock-avoidance-timeout=0 dedupv1_contrib_test | ./filter_path.py --stdin

valgrind_dedupv1d_test:
	scons --no-tc-malloc --no-tbb-malloc --deadlock-avoidance-timeout=0 dedupv1d_test | ./filter_path.py --stdin

research:
	scons --research all | ./filter_path.py --stdin

stylecheck:
	export DEDUPV1_ROOT=`pwd`
	echo ${DEDUPV1_ROOT}
	thirdparty/cpplint/cpplint
