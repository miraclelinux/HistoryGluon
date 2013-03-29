require "mkmf"
find_library 'history-gluon', 'history_gluon_create_context'
create_makefile("historygluon")
