add_subdirectory(egen)
add_subdirectory(record)

set_property(GLOBAL APPEND PROPERTY ALL_SRC
#  ${CMAKE_CURRENT_SOURCE_DIR}/tpce.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/oddlb.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/oddlb-config.cc  
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc-common.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc-ddls.cc
  #${CMAKE_CURRENT_SOURCE_DIR}/tpcc-cs.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/ycsb-config.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/ycsb.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/ycsb-cs.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/ycsb-cs-advance.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/bench.cc
)
