add_library(authTls MODULE AuthTls.cc)
target_link_libraries(authTls ${CLIENT_LIBS})
set_target_properties(authTls PROPERTIES OUTPUT_NAME authtls)
