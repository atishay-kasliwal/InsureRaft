cmake_minimum_required(VERSION 3.16)
project(InsureRaft VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 11)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "" FORCE)
endif()

# ── NuRaft: pull in as a subdirectory, skip its tests and examples ────────────
set(BUILD_TESTING  OFF CACHE BOOL "" FORCE)
set(BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
add_subdirectory(deps/nuraft)

# ── InsureRaft server ─────────────────────────────────────────────────────────
add_executable(insurance_server
    src/insurance_server.cxx
    src/insurance_log_store.cxx
    deps/nuraft/examples/logger.cc
)

target_include_directories(insurance_server PRIVATE
    src
    deps/nuraft/include/libnuraft
    deps/nuraft/examples
    deps/nuraft/asio/asio/include
)

target_compile_definitions(insurance_server PRIVATE ASIO_STANDALONE)

# static_lib is NuRaft's CMake target; it already carries OpenSSL as a
# transitive PUBLIC dependency so we only need to link against it.
target_link_libraries(insurance_server PRIVATE static_lib)
