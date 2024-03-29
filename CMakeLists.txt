# No in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")

project(corobase)
cmake_minimum_required(VERSION 3.1)

if(${CMAKE_C_COMPILER} MATCHES "clang+")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -std=c++14 -fcoroutines-ts -stdlib=libc++ -Woverloaded-virtual -Werror=reorder -Werror=unused-result -Wno-address-of-packed-member")
  
  include_directories(/opt/dis/llvm-libc++/include/c++/v1)
  link_directories(/opt/dis/llvm-libc++/lib)
  
  # Pre-build glog and gflags with libc++
  link_directories(${CMAKE_CURRENT_SOURCE_DIR}/third-party/glog/lib)
  link_directories(${CMAKE_CURRENT_SOURCE_DIR}/third-party/gflags/lib)

  # The required headers for libc++.
  include_directories(third-party/glog/include)
  include_directories(third-party/gflags/include)
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -std=c++14 -fcoroutines -Woverloaded-virtual -Werror=reorder -Wno-address-of-packed-member -Wno-format-truncation -Wno-unused-result")
  
  # link glog for gcc.
  link_directories(/usr/local/lib)
endif()


set(LINK_FLAGS "-lc++abi -lnuma -lpthread -pthread -lrt -lglog -lgflags -luring")
link_libraries(${LINK_FLAGS})


# Store shared libraries under /lib
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# Use masstree for index
add_definitions(-DMASSTREE)
# Assume 64-byte cache line
add_definitions(-DCACHELINE_SIZE=64)

set(MASSTREE_CONFIG "--enable-max-key-len=1024 --with-malloc=malloc")

if(${CMAKE_BUILD_TYPE} STREQUAL "Debug")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -include ${CMAKE_CURRENT_SOURCE_DIR}/masstree/config-debug.h")
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -include ${CMAKE_CURRENT_SOURCE_DIR}/masstree/config-release.h")
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")
endif()

include_directories(dbcore)
include_directories(masstree)
include_directories(third-party/sparsehash/src)

add_subdirectory(benchmarks)
add_subdirectory(dbcore)
add_subdirectory(masstree)

set_property(GLOBAL APPEND PROPERTY ALL_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/engine.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/corobase.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/tuple.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/txn.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/txn-ssn.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/txn-ssi.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/txn-mvocc.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/catalog_mgr.cc
)

get_property(LIB_SRC GLOBAL PROPERTY ALL_SRC)

# Thread pool source
set_property(GLOBAL APPEND PROPERTY THREAD_POOL_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/dbcore/sm-thread.cpp
)
get_property(THREAD_POOL_SRC GLOBAL PROPERTY THREAD_POOL_SRC)


# Thread pool library
add_library(thread_pool SHARED ${THREAD_POOL_SRC})

# Non blocking DDL - SI
add_library(tesseract_ddl_si SHARED ${LIB_SRC})
set_target_properties(tesseract_ddl_si PROPERTIES COMPILE_FLAGS "-DDDL -DSIDDL")

add_executable(tesseract_DDL_SI ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/dbtest.cc)
set_target_properties(tesseract_DDL_SI PROPERTIES COMPILE_FLAGS "-DDDL -DSIDDL")
target_link_libraries(tesseract_DDL_SI tesseract_ddl_si thread_pool)

# Non blocking DDL - Copy
add_library(tesseract_ddl_copy SHARED ${LIB_SRC})
set_target_properties(tesseract_ddl_copy PROPERTIES COMPILE_FLAGS "-DDDL -DCOPYDDL")

add_executable(tesseract_DDL_COPY ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/dbtest.cc)
set_target_properties(tesseract_DDL_COPY PROPERTIES COMPILE_FLAGS "-DDDL -DCOPYDDL")
target_link_libraries(tesseract_DDL_COPY tesseract_ddl_copy thread_pool)

# Non blocking DDL - Lazy copy
add_library(tesseract_ddl_lazy_copy SHARED ${LIB_SRC})
set_target_properties(tesseract_ddl_lazy_copy PROPERTIES COMPILE_FLAGS "-DDDL -DCOPYDDL -DLAZYDDL")

add_executable(tesseract_DDL_LAZY_COPY ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/dbtest.cc)
set_target_properties(tesseract_DDL_LAZY_COPY PROPERTIES COMPILE_FLAGS "-DDDL -DCOPYDDL -DLAZYDDL")
target_link_libraries(tesseract_DDL_LAZY_COPY tesseract_ddl_lazy_copy thread_pool)

# Non blocking DDL - Optimized lazy copy
add_library(tesseract_ddl_opt_lazy_copy SHARED ${LIB_SRC})
set_target_properties(tesseract_ddl_opt_lazy_copy PROPERTIES COMPILE_FLAGS "-DDDL -DCOPYDDL -DLAZYDDL -DOPTLAZYDDL")

add_executable(tesseract_DDL_OPT_LAZY_COPY ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/dbtest.cc)
set_target_properties(tesseract_DDL_OPT_LAZY_COPY PROPERTIES COMPILE_FLAGS "-DDDL -DCOPYDDL -DLAZYDDL -DOPTLAZYDDL")
target_link_libraries(tesseract_DDL_OPT_LAZY_COPY tesseract_ddl_opt_lazy_copy thread_pool)

# Blocking DDL
add_library(tesseract_ddl_block SHARED ${LIB_SRC})
set_target_properties(tesseract_ddl_block PROPERTIES COMPILE_FLAGS "-DDDL -DBLOCKDDL")

add_executable(tesseract_DDL_BLOCK ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/dbtest.cc)
set_target_properties(tesseract_DDL_BLOCK PROPERTIES COMPILE_FLAGS "-DDDL -DBLOCKDDL")
target_link_libraries(tesseract_DDL_BLOCK tesseract_ddl_block thread_pool)

# Baseline without DDL 
add_library(tesseract_no_ddl SHARED ${LIB_SRC})
set_target_properties(tesseract_no_ddl PROPERTIES COMPILE_FLAGS "")

add_executable(tesseract_NO_DDL ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/dbtest.cc)
set_target_properties(tesseract_NO_DDL PROPERTIES COMPILE_FLAGS "")
target_link_libraries(tesseract_NO_DDL tesseract_no_ddl thread_pool)

# SI
#add_library(corobase_si SHARED ${LIB_SRC})
#set_target_properties(corobase_si PROPERTIES COMPILE_FLAGS "")
#set_target_properties(corobase_si PROPERTIES COMPILE_FLAGS "-DCORO_BATCH_COMMIT -DBATCH_SAME_TRX -DCORO_UPDATE_VERSION_CHAIN")

#add_executable(corobase_SI ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/dbtest.cc)
#set_target_properties(corobase_SI PROPERTIES COMPILE_FLAGS "")
#set_target_properties(corobase_SI PROPERTIES COMPILE_FLAGS "-DCORO_BATCH_COMMIT -DBATCH_SAME_TRX -DCORO_UPDATE_VERSION_CHAIN")
#target_link_libraries(corobase_SI corobase_si thread_pool)

# SI with fully nested coroutine
#add_library(corobase_adv_coro_si SHARED ${LIB_SRC})
#set_target_properties(corobase_adv_coro_si PROPERTIES COMPILE_FLAGS "-DADV_COROUTINE")

#add_executable(corobase_adv_coro_SI ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/dbtest.cc)
#set_target_properties(corobase_adv_coro_SI PROPERTIES COMPILE_FLAGS "-DADV_COROUTINE")
#target_link_libraries(corobase_adv_coro_SI corobase_adv_coro_si thread_pool)

# Thread pool library with SSN enabled
#add_library(thread_pool_ssn SHARED ${THREAD_POOL_SRC})
#set_target_properties(thread_pool_ssn PROPERTIES COMPILE_FLAGS "-DSSN -DEARLY_SSN_CHECK")

# SI+SSN
#add_library(corobase_si_ssn SHARED ${LIB_SRC})
#set_target_properties(corobase_si_ssn PROPERTIES COMPILE_FLAGS "-DSSN -DEARLY_SSN_CHECK")

#add_executable(corobase_SI_SSN ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/dbtest.cc)
#set_target_properties(corobase_SI_SSN PROPERTIES COMPILE_FLAGS "-DSSN -DEARLY_SSN_CHECK")
#target_link_libraries(corobase_SI_SSN corobase_si_ssn thread_pool_ssn)

# SI+SSN with fully nested coroutine
#add_library(corobase_adv_coro_si_ssn SHARED ${LIB_SRC})
#set_target_properties(corobase_adv_coro_si_ssn PROPERTIES COMPILE_FLAGS "-DSSN -DEARLY_SSN_CHECK -DADV_COROUTINE")

#add_executable(corobase_adv_coro_SI_SSN ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/dbtest.cc)
#set_target_properties(corobase_adv_coro_SI_SSN PROPERTIES COMPILE_FLAGS "-DSSN -DEARLY_SSN_CHECK -DADV_COROUTINE")
#target_link_libraries(corobase_adv_coro_SI_SSN corobase_adv_coro_si_ssn thread_pool_ssn)

# SSI
#add_library(ermia_ssi SHARED ${LIB_SRC})
#set_target_properties(ermia_ssi PROPERTIES COMPILE_FLAGS "-DSSI")

#add_executable(ermia_SSI ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/dbtest.cc)
#set_target_properties(ermia_SSI PROPERTIES COMPILE_FLAGS "-DSSI")
#target_link_libraries(ermia_SSI ermia_ssi thread_pool)

# MVOCC
#add_library(ermia_mvocc SHARED ${LIB_SRC})
#set_target_properties(ermia_mvocc PROPERTIES COMPILE_FLAGS "-DMVOCC")

#add_executable(ermia_MVOCC ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/dbtest.cc)
#set_target_properties(ermia_MVOCC PROPERTIES COMPILE_FLAGS "-DMVOCC")
#target_link_libraries(ermia_MVOCC ermia_mvocc thread_pool)

# Benchmark scripts
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/run.sh" DESTINATION ${CMAKE_BINARY_DIR})

enable_testing()

if(BUILD_TESTS)
    set(BUILD_GMOCK OFF CACHE BOOL "Gmock Not Used")
    set(INSTALL_GTEST OFF CACHE BOOL "Static Link Gtest")

    set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "No Testing For GoogleBenchmark Itself")
    set(BENCHMARK_ENABLE_EXECEPTIONS OFF CACHE BOOL "No Exception For GoogleBenchmark")
    set(BENCHMARK_ENABLE_LTO OFF CACHE BOOL "FIXME: Link Time Optimization leads to linking error")
    set(BENCHMARK_USE_LIBCXX ON CACHE BOOL "Build GoogleBenchmark With libc++")
    set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "Static Link GoogleBenchmark")

    add_subdirectory(tests)
endif()
