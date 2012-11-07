/* History Gluon
   Copyright (C) 2012 MIRACLE LINUX CORPRATION
 
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

#ifndef _message_h
#define _message_h

void history_gluon_message(char *fmt, ...);
void history_gluon_error(char *fmt, ...);

#define INFO(FMT, ...) \
(history_gluon_message("%s: %d: " FMT, __FILE__, __LINE__, ##__VA_ARGS__))

#define ERR_MSG(FMT, ...) \
(history_gluon_error("%s: %d: " FMT, __FILE__, __LINE__, ##__VA_ARGS__))

#endif //_message_h
