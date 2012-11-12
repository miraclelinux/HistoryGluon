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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <glib.h>
#include "message.h"

enum {
	LV_INIT = -1,
	LV_DBG  = 0,
	LV_INFO = 10,
	LV_WARN = 20,
	LV_ERR  = 30,
	LV_CRIT = 40,
	LV_NONE = 1000000,
};

static const int DEFAULT_MSG_LV = LV_INFO;
static volatile int g_message_level = LV_INIT;

static int set_message_level(void)
{
	int level = DEFAULT_MSG_LV;
	char *level_str = getenv("HISTORY_GLUON_MSG_LV");
	if (level_str) {
		if (strcmp(level_str, "DBG") == 0)
			level = LV_DBG;
		else if (strcmp(level_str, "INFO") == 0)
			level = LV_INFO;
		else if (strcmp(level_str, "WARN") == 0)
			level = LV_WARN;
		else if (strcmp(level_str, "ERR") == 0)
			level = LV_ERR;
		else if (strcmp(level_str, "CRIT") == 0)
			level = LV_CRIT;
		else if (strcmp(level_str, "NONE") == 0)
			level = LV_NONE;
	}
	g_atomic_int_set(&g_message_level, level);
	return level;
}

static int check_level(int print_level)
{
	int msg_level = g_atomic_int_get(&g_message_level);
	if (msg_level == LV_INIT)
		msg_level = set_message_level();
	return (print_level >= msg_level ? 1 : 0);
}

void history_gluon_message(char *fmt, ...)
{
	if (check_level(LV_INFO) == 0)
		return;

	va_list ap;
	va_start(ap, fmt); 
	vfprintf(stdout, fmt, ap);
	va_end(ap);
}

void history_gluon_error(char *fmt, ...)
{
	if (check_level(LV_ERR) == 0)
		return;

	va_list ap;
	va_start(ap, fmt); 
	vfprintf(stderr, fmt, ap);
	va_end(ap);
}
