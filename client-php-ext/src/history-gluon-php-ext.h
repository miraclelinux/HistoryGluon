/* History Gluon
   Copyright (C) 2012 MIRACLE LINUX CORPORATION
 
   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef PHP_HISTORY_GLUON_H
#define PHP_HISTORY_GLUON_H
#define PHP_HISTORY_GLUON_VERSION "0.1"
#define PHP_HISTORY_GLUON_NAME "History Gluon PHP Extension"

PHP_MINIT_FUNCTION(history_gluon);
PHP_MSHUTDOWN_FUNCTION(history_gluon);
PHP_RINIT_FUNCTION(history_gluon);

PHP_FUNCTION(history_gluon_create_context);
PHP_FUNCTION(history_gluon_free_context);
PHP_FUNCTION(history_gluon_add_uint);
PHP_FUNCTION(history_gluon_add_float);
PHP_FUNCTION(history_gluon_add_string);
PHP_FUNCTION(history_gluon_add_blob);
PHP_FUNCTION(history_gluon_range_query);
PHP_FUNCTION(history_gluon_delete);

extern zend_module_entry php_history_gluon_module_entry;
#define phpext_history_gluon_ptr &php_history_gluon_module_entry

#endif
