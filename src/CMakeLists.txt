# systemd
set(SD_SOURCES sd-daemon.c)
set(SD_HEADERS sd-daemon.h)
add_library(sd-daemon STATIC ${SD_SOURCES})
set(SD_LIBRARIES sd-daemon)

# kollektd
add_executable(kollektd main.cpp)
set_target_properties(kollektd PROPERTIES COMPILE_FLAGS "-std=c++0x")
target_link_libraries(kollektd ${SD_LIBRARIES} ${EV_LIBRARIES})
