#ifndef PHP_HISTORY_GLUON_H
#define PHP_HISTORY_GLUON_H
#define PHP_HISTORY_GLUON_VERSION "0.1"
#define PHP_HISTORY_GLUON_NAME "History Gluon PHP Extension"

PHP_MINIT_FUNCTION(history_gluon);
PHP_MSHUTDOWN_FUNCTION(history_gluon);
PHP_RINIT_FUNCTION(history_gluon);

PHP_FUNCTION(create_context);

extern zend_module_entry php_history_gluon_module_entry;
#define phpext_history_gluon_ptr &php_history_gluon_module_entry

#endif
