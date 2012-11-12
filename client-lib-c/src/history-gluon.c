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
#include <stdint.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <errno.h>
#include <string.h>
#include <inttypes.h>

#include "history-gluon.h"
#include "message.h"

#define DEFAULT_PORT 30010
#define DEFAULT_SERVER_NAME "localhost"

#define MAX_STRING_LENGTH 0x7fffffff
#define MAX_BLOB_LENGTH   0x7fffffffffffffff

#define READ_CHUNK_SIZE   0x10000

#define RETURN_IF_ERROR(R) \
do { \
	if (R != HGL_SUCCESS) \
		return R; \
} while(0)

#define GOTO_IF_ERROR(R,L) \
do { \
	if (R != HGL_SUCCESS) \
		goto L; \
} while(0)

#define BREAK_IF_ERROR(R) \
do { \
	if (R != HGL_SUCCESS) \
		break; \
} while(0)

/* Connection */
#define MAGIC_CODE_LENGTH         4
#define DB_NAME_SIZE_LENGTH       2

/* Common header */
#define PKT_SIZE_LENGTH           4
#define PKT_CMD_TYPE_LENGTH       2
#define PKT_DATA_TYPE_LENGTH      2
#define PKT_ID_LENGTH             8
#define PKT_SEC_LENGTH            4
#define PKT_NS_LENGTH             4

/* Common reply */
#define REPLY_RESULT_LENGTH       4

/* Add Data */
#define PKT_DATA_FLOAT_LENGTH        8
#define PKT_DATA_UINT_LENGTH         8
#define PKT_DATA_STRING_SIZE_LENGTH  4
#define PKT_DATA_BLOB_SIZE_LENGTH    8

#define PKT_ADD_DATA_HEADER_LENGTH \
(PKT_SIZE_LENGTH + \
 PKT_CMD_TYPE_LENGTH + \
 PKT_DATA_TYPE_LENGTH + \
 PKT_ID_LENGTH + \
 PKT_SEC_LENGTH + \
 PKT_NS_LENGTH)

#define REPLY_ADD_LENGTH \
(PKT_SIZE_LENGTH + \
 PKT_CMD_TYPE_LENGTH + \
 REPLY_RESULT_LENGTH)

/* Query Data */
#define PKT_QUERY_TYPE_LENGTH 2

#define PKT_QUERY_DATA_LENGTH \
(PKT_SIZE_LENGTH + \
 PKT_CMD_TYPE_LENGTH + \
 PKT_ID_LENGTH + \
 PKT_SEC_LENGTH + \
 PKT_NS_LENGTH + \
 PKT_QUERY_TYPE_LENGTH)

#define REPLY_QUERY_DATA_HEADER_LENGTH \
(PKT_SIZE_LENGTH + \
 PKT_CMD_TYPE_LENGTH + \
 REPLY_RESULT_LENGTH)

/* Range Query */
#define PKT_NUM_ENTRIES_LENGTH 8
#define PKT_SORT_ORDER_LENGTH  2

#define PKT_RANGE_QUERY_LENGTH \
(PKT_SIZE_LENGTH + \
 PKT_CMD_TYPE_LENGTH + \
 PKT_ID_LENGTH + \
 2 * (PKT_SEC_LENGTH + PKT_NS_LENGTH) + \
 PKT_NUM_ENTRIES_LENGTH + \
 PKT_SORT_ORDER_LENGTH)

#define REPLY_RANGE_QUERY_HEADER_LENGTH \
(PKT_SIZE_LENGTH + \
 PKT_CMD_TYPE_LENGTH + \
 REPLY_RESULT_LENGTH + \
 PKT_NUM_ENTRIES_LENGTH + \
 PKT_SORT_ORDER_LENGTH)

/* Query All */
#define PKT_QUERY_ALL_LENGTH   (PKT_SIZE_LENGTH + PKT_CMD_TYPE_LENGTH)
#define REPLY_QUERY_ALL_LENGTH (PKT_SIZE_LENGTH + PKT_CMD_TYPE_LENGTH)

/* Get Minimum Time */
#define PKT_GET_MIN_TIME_LENGTH \
(PKT_SIZE_LENGTH + \
 PKT_CMD_TYPE_LENGTH + \
 PKT_ID_LENGTH)

#define REPLY_GET_MIN_TIME_LENGTH \
(PKT_SIZE_LENGTH + \
 PKT_CMD_TYPE_LENGTH + \
 REPLY_RESULT_LENGTH + \
 PKT_SEC_LENGTH + \
 PKT_NS_LENGTH)

/* Get Statistics */
#define PKT_GET_STATISTICS_LENGTH \
(PKT_SIZE_LENGTH + \
 PKT_CMD_TYPE_LENGTH + \
 PKT_ID_LENGTH + \
 2 * (PKT_SEC_LENGTH + PKT_NS_LENGTH))

#define REPLY_STATISTICS_LENGTH \
(PKT_SIZE_LENGTH + \
 PKT_CMD_TYPE_LENGTH + \
 REPLY_RESULT_LENGTH + \
 PKT_ID_LENGTH + \
 PKT_DATA_UINT_LENGTH + \
 3 * PKT_DATA_FLOAT_LENGTH)

/* Delete Data */
#define  PKT_DELETE_WAY_LENGTH 2

#define PKT_DELETE_LENGTH \
(PKT_SIZE_LENGTH + \
 PKT_CMD_TYPE_LENGTH + \
 PKT_ID_LENGTH + \
 PKT_SEC_LENGTH + \
 PKT_NS_LENGTH + \
 PKT_DELETE_WAY_LENGTH)

#define REPLY_COUNT_DELETED_LENGTH 8

#define REPLY_DELETE_LENGTH \
(PKT_SIZE_LENGTH + \
 PKT_CMD_TYPE_LENGTH + \
 REPLY_RESULT_LENGTH + \
 REPLY_COUNT_DELETED_LENGTH)

/* Packet type */
enum {
	PKT_CMD_ADD_DATA           = 100,
	PKT_CMD_QUERY_DATA         = 200,
	PKT_CMD_RANGE_QUERY        = 300,
	PKT_CMD_QUERY_ALL          = 310,
	PKT_CMD_GET_MINIMUM_TIME   = 400,
	PKT_CMD_GET_STATISTICS     = 500,
	PKT_CMD_DELETE             = 600,
};

/* Result code */
enum {
	RESULT_SUCCESS = 0,
	RESULT_ERROR_UNKNOWN_REASON = 1,
	RESULT_ERROR_TOO_MANY_RECORDS = 2,
};

typedef struct
{
	int connected;
	char *server_name;
	int port;
	int socket;
	int endian;
	char *db_name;
}
private_context_t;

typedef history_gluon_result_t (*length_check_func_t)(void);
#define LENGTH_CHECK_NONE 0

/* ---------------------------------------------------------------------------
 * Global constants
 * ------------------------------------------------------------------------- */
struct timespec HISTORY_GLUON_TIMESPEC_START = {0, 0};
struct timespec HISTORY_GLUON_TIMESPEC_END = {0xffffffff, 0xffffffff};

/* ---------------------------------------------------------------------------
 * Private methods
 * ------------------------------------------------------------------------- */
static const uint8_t
connect_magic_code[MAGIC_CODE_LENGTH] = {'H', 'G', 'L', '\0'};

static const char valid_db_name_char_array[0x100] = {
  ['A'] = 1, ['B'] = 1, ['C'] = 1, ['D'] = 1, ['E'] = 1, ['F'] = 1, ['G'] = 1,
  ['H'] = 1, ['I'] = 1, ['J'] = 1, ['K'] = 1, ['L'] = 1, ['M'] = 1, ['N'] = 1,
  ['O'] = 1, ['P'] = 1, ['Q'] = 1, ['R'] = 1, ['S'] = 1, ['T'] = 1, ['U'] = 1,
  ['V'] = 1, ['W'] = 1, ['X'] = 1, ['Y'] = 1, ['Z'] = 1,
  ['a'] = 1, ['b'] = 1, ['c'] = 1, ['d'] = 1, ['e'] = 1, ['f'] = 1, ['g'] = 1,
  ['h'] = 1, ['i'] = 1, ['j'] = 1, ['k'] = 1, ['l'] = 1, ['m'] = 1, ['n'] = 1,
  ['o'] = 1, ['p'] = 1, ['q'] = 1, ['r'] = 1, ['s'] = 1, ['t'] = 1, ['u'] = 1,
  ['v'] = 1, ['w'] = 1, ['x'] = 1, ['y'] = 1, ['z'] = 1,
  ['0'] = 1, ['1'] = 1, ['2'] = 1, ['3'] = 1, ['4'] = 1, ['5'] = 1, ['6'] = 1,
  ['7'] = 1, ['8'] = 1, ['9'] = 1,
  ['.'] = 1, ['_'] = 1, ['-'] = 1, ['@'] = 1, ['/'] = 1,
  /* The linker shall fill 0 for other characters, because this is
     static varible region */
};

static history_gluon_result_t check_database_name(const char *database_name)
{
	if (!database_name)
		return HGLERR_INVALID_DB_NAME;
	size_t len = strlen(database_name);
	if (len > HISTORY_GLUON_MAX_DATABASE_NAME_LENGTH) {
		ERR_MSG("Too long database name: %zd\n", len);
		return HGLERR_TOO_LONG_DB_NAME;
	}
	size_t i;
	for (i = 0; i < len; i++) {
		const unsigned char c = database_name[i];
		if (!valid_db_name_char_array[c]) {
			ERR_MSG("invalid char: %02x at %d\n", c, i);
			return HGLERR_INVALID_DB_NAME;
		}
	}
	return HGL_SUCCESS;
}

static int get_system_endian(void)
{
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
	return LITTLE_ENDIAN;
#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
	return BIG_ENDIAN;
#else
#error Failed to detect byte order
#endif
}

static void reverse_byte_order(uint8_t *dest, uint8_t *src, int size)
{
	int i;
	for (i = 0; i < size; i++)
		dest[i] = src[size - i - 1];
}

static uint64_t conv_le64(private_context_t *ctx, uint64_t val)
{
	uint64_t ret;
	if (ctx->endian == LITTLE_ENDIAN)
		return val;
	reverse_byte_order((uint8_t *)&ret, (uint8_t *)&val, 8);
	return ret;
}

static uint32_t conv_le32(private_context_t *ctx, uint32_t val)
{
	uint32_t ret;
	if (ctx->endian == LITTLE_ENDIAN)
		return val;
	reverse_byte_order((uint8_t *)&ret, (uint8_t *)&val, 4);
	return ret;
}

static uint16_t conv_le16(private_context_t *ctx, uint16_t val)
{
	uint16_t ret = val;
	if (ctx->endian == LITTLE_ENDIAN)
		return val;
	reverse_byte_order((uint8_t *)&ret, (uint8_t *)&val, 2);
	return ret;
}

static uint64_t restore_le64(private_context_t *ctx, void *buf)
{
	return conv_le64(ctx, *((uint64_t *)buf));
}

static uint32_t restore_le32(private_context_t *ctx, void *buf)
{
	return conv_le32(ctx, *((uint32_t *)buf));
}

static uint16_t restore_le16(private_context_t *ctx, void *buf)
{
	return conv_le16(ctx, *((uint16_t *)buf));
}

static void write_ieee754_double(private_context_t *ctx, uint8_t *buf, double val)
{
	// TODO: add procedure for the platform whose floating point is not IEEE745
	memcpy(buf, (const void *)&val, 8);
}

static double read_ieee754_double(private_context_t *ctx, uint8_t *buf)
{
	// TODO: add procedure for the platform whose floating point is not IEEE745
	double d;
	memcpy(&d, buf, 8);
	return d;
}

static void reset_context(private_context_t *ctx)
{
	if (ctx->socket != -1) {
		close(ctx->socket);
		ctx->socket = -1;
	}
	ctx->connected = 0;
}

static history_gluon_result_t
write_data(private_context_t *ctx, uint8_t *buf, uint64_t count)
{
	uint8_t *ptr = buf;
	while (1) {
		ssize_t written_byte = write(ctx->socket, ptr, count);
		if (written_byte == -1) {
			ERR_MSG("Failed to write data: %d\n", errno);
			reset_context(ctx);
			return HGLERR_WRITE_ERROR;
		}
		if (written_byte >= count)
			break;
		count -= written_byte;
		ptr += written_byte;
	}
	return HGL_SUCCESS;
}

static history_gluon_result_t
read_data(private_context_t *ctx, uint8_t *buf, uint64_t count)
{
	uint8_t *ptr = buf;
	while (1) {
		size_t read_request_count = READ_CHUNK_SIZE;
		if (count < read_request_count)
			read_request_count = count;
		ssize_t read_byte = read(ctx->socket, ptr, read_request_count);
		if (read_byte == 0) {
			ERR_MSG("file stream has unexpectedly closed @ "
			        "remaing count: %" PRIu64 "\n", count);
			reset_context(ctx);
			return HGLERR_READ_STREAM_END;
		} else if (read_byte == -1) {
			ERR_MSG("Failed to read data: %d\n", errno);
			reset_context(ctx);
			return HGLERR_READ_ERROR;
		}
		if (read_byte >= count)
			break;
		count -= read_byte;
		ptr += read_byte;
	}
	return HGL_SUCCESS;
}

static history_gluon_result_t
proc_connect(private_context_t *ctx)
{
	history_gluon_result_t ret;

	/* magic code */
	ret = write_data(ctx, (uint8_t *)connect_magic_code, MAGIC_CODE_LENGTH);
	RETURN_IF_ERROR(ret);

	/* length of DB name */
	uint16_t len_db_name = strlen(ctx->db_name);
	uint16_t le_buf_length = conv_le16(ctx, len_db_name);
	ret = write_data(ctx, (uint8_t *)&le_buf_length, DB_NAME_SIZE_LENGTH);
	RETURN_IF_ERROR(ret);

	/* DB name */
	ret = write_data(ctx, (uint8_t *)ctx->db_name, len_db_name);
	RETURN_IF_ERROR(ret);

	/* wait for reply */
	int reply_len = MAGIC_CODE_LENGTH + REPLY_RESULT_LENGTH;
	uint8_t reply[reply_len];
	ret = read_data(ctx, reply, reply_len);
	RETURN_IF_ERROR(ret);

	/* magic code */
	int idx = 0;
	if (memcmp(connect_magic_code, &reply[idx], MAGIC_CODE_LENGTH) != 0) {
		ERR_MSG("Unexpected reply magic code: %02x %02x %02x %02x\n",
		        reply[0], reply[1], reply[2], reply[3]);
		reset_context(ctx);
		return HGLERR_UNEXPECTED_MAGIC_CODE;
	}
	idx += MAGIC_CODE_LENGTH;

	/* result */
	uint32_t result = restore_le32(ctx, &reply[idx]);
	idx += REPLY_RESULT_LENGTH;

	if (result != HGLSV_SUCCESS) {
		ERR_MSG("Connect process failed: %d\n", result);
		reset_context(ctx);
	}

	return result;
}

static history_gluon_result_t
connect_to_history_service(private_context_t *ctx)
{
	/* crate the string for the port */
	static const int LEN_PORT_STR = 10;
	char port_str[LEN_PORT_STR];
	snprintf(port_str, LEN_PORT_STR, "%d", ctx->port);

	/* get the address info */
	int sock;
	struct addrinfo* rp;
	struct addrinfo hints;
	struct addrinfo* result = NULL;
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_NUMERICSERV;
	if (getaddrinfo(ctx->server_name, port_str, &hints, &result) != 0) {
		ERR_MSG("Failed to call getaddrinfo(): errno: %d, host: %s, port: %s\n",
		        errno, ctx->server_name, port_str);
		return HGLERR_GETADDRINFO;
	}

	/* try to connect to the addresses returned in the above */
	for (rp = result; rp != NULL; rp = rp->ai_next) {
		sock = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
		if (sock == -1)
			continue;
		if (connect(sock, rp->ai_addr, rp->ai_addrlen) != -1)
			break; /* Success */
		close(sock);
	}

	freeaddrinfo(result);
	if (rp == NULL) {
		ERR_MSG("Failed to connect: host: %s, port: %s\n",
		        ctx->server_name, port_str);
		return HGLERR_FAILED_CONNECT;
	}

	ctx->connected = 1;
	ctx->socket = sock;
	INFO("Connected to history server: host: %s, port: %s\n",
	     ctx->server_name, port_str);

	return proc_connect(ctx);
}

static history_gluon_result_t
get_connected_private_context(history_gluon_context_t ctx,
                              private_context_t **ctx_out)
{
	private_context_t *priv_ctx = ctx;
	if (priv_ctx->connected) {
		*ctx_out = priv_ctx;
		return HGL_SUCCESS;
	}

	history_gluon_result_t ret = connect_to_history_service(priv_ctx);
	if (ret == HGL_SUCCESS)
		*ctx_out = priv_ctx;
	return ret;
}

static int fill_add_data_header(private_context_t *ctx, uint64_t id,
                                struct timespec *ts, uint8_t *buf,
                                uint16_t data_type, uint32_t pkt_size)
{
	int idx = 0;

	/* pkt size */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, pkt_size - PKT_SIZE_LENGTH);
	idx += PKT_SIZE_LENGTH;

	/* command */
	*((uint16_t *)&buf[idx]) = conv_le16(ctx, PKT_CMD_ADD_DATA);
	idx += PKT_CMD_TYPE_LENGTH;

	/* data type */
	*((uint16_t *)&buf[idx]) = conv_le16(ctx, data_type);
	idx += PKT_DATA_TYPE_LENGTH;

	/* ID */
	*((uint64_t *)&buf[idx]) = conv_le64(ctx, id);
	idx += PKT_ID_LENGTH;

	/* sec */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, ts->tv_sec);
	idx += PKT_SEC_LENGTH;

	/* ns */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, ts->tv_nsec);
	idx += PKT_NS_LENGTH;

	return idx;
}

static int fill_query_data_header(private_context_t *ctx, uint64_t id,
                                  struct timespec *ts, uint8_t *buf,
                                  history_gluon_query_t query_type)
{
	int idx = 0;

	/* pkt size */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, PKT_QUERY_DATA_LENGTH - PKT_SIZE_LENGTH);
	idx += PKT_SIZE_LENGTH;

	/* command */
	*((uint16_t *)&buf[idx]) = conv_le16(ctx, PKT_CMD_QUERY_DATA);
	idx += PKT_CMD_TYPE_LENGTH;

	/* ID */
	*((uint64_t *)&buf[idx]) = conv_le64(ctx, id);
	idx += PKT_ID_LENGTH;

	/* sec */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, ts->tv_sec);
	idx += PKT_SEC_LENGTH;

	/* ns */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, ts->tv_nsec);
	idx += PKT_NS_LENGTH;

	/* query type  */
	*((uint16_t *)&buf[idx]) = conv_le16(ctx, query_type);
	idx += PKT_QUERY_TYPE_LENGTH;

	return idx;
}

static int fill_range_query_header(private_context_t *ctx, uint8_t *buf, uint64_t id,
                                   struct timespec *ts0, struct timespec *ts1,
                                   history_gluon_sort_order_t sort_request,
                                   uint64_t num_max_entries)
{
	int idx = 0;

	/* pkt size */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, PKT_RANGE_QUERY_LENGTH - PKT_SIZE_LENGTH);
	idx += PKT_SIZE_LENGTH;

	/* command */
	*((uint16_t *)&buf[idx]) = conv_le16(ctx, PKT_CMD_RANGE_QUERY);
	idx += PKT_CMD_TYPE_LENGTH;

	/* ID */
	*((uint64_t *)&buf[idx]) = conv_le64(ctx, id);
	idx += PKT_ID_LENGTH;

	/* ts0: sec */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, ts0->tv_sec);
	idx += PKT_SEC_LENGTH;

	/* ts0: ns */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, ts0->tv_nsec);
	idx += PKT_NS_LENGTH;

	/* ts1: sec */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, ts1->tv_sec);
	idx += PKT_SEC_LENGTH;

	/* ts1: ns */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, ts1->tv_nsec);
	idx += PKT_NS_LENGTH;

	/* maximum entries  */
	*((uint64_t *)&buf[idx]) = conv_le64(ctx, num_max_entries);
	idx += PKT_NUM_ENTRIES_LENGTH;

	/* sort order request  */
	*((uint16_t *)&buf[idx]) = conv_le16(ctx, sort_request);
	idx += PKT_SORT_ORDER_LENGTH;

	return idx;
}

static int fill_query_all_packet(private_context_t *ctx, uint8_t *buf)
{
	int idx = 0;

	/* pkt size */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, PKT_QUERY_ALL_LENGTH - PKT_SIZE_LENGTH);
	idx += PKT_SIZE_LENGTH;

	/* command */
	*((uint16_t *)&buf[idx]) = conv_le16(ctx, PKT_CMD_QUERY_ALL);
	idx += PKT_CMD_TYPE_LENGTH;

	return idx;
}


static int fill_get_minimum_time_packet(private_context_t *ctx, uint8_t *buf, uint64_t id)
{
	int idx = 0;

	/* pkt size */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, PKT_GET_MIN_TIME_LENGTH - PKT_SIZE_LENGTH);
	idx += PKT_SIZE_LENGTH;

	/* command */
	*((uint16_t *)&buf[idx]) = conv_le16(ctx, PKT_CMD_GET_MINIMUM_TIME);
	idx += PKT_CMD_TYPE_LENGTH;

	/* ID */
	*((uint64_t *)&buf[idx]) = conv_le64(ctx, id);
	idx += PKT_ID_LENGTH;

	return idx;
}

static int fill_delete_packet(private_context_t *ctx, uint8_t *buf, uint64_t id,
                              struct timespec *ts, history_gluon_delete_way_t delete_way)
{
	int idx = 0;

	/* pkt size */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, PKT_DELETE_LENGTH - PKT_SIZE_LENGTH);
	idx += PKT_SIZE_LENGTH;

	/* command */
	*((uint16_t *)&buf[idx]) = conv_le16(ctx, PKT_CMD_DELETE);
	idx += PKT_CMD_TYPE_LENGTH;

	/* ID */
	*((uint64_t *)&buf[idx]) = conv_le64(ctx, id);
	idx += PKT_ID_LENGTH;

	/* second */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, ts->tv_sec);
	idx += PKT_SEC_LENGTH;

	/* nano second */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, ts->tv_nsec);
	idx += PKT_NS_LENGTH;

	/* delete way */
	*((uint16_t *)&buf[idx]) = conv_le16(ctx, delete_way);
	idx += PKT_DELETE_WAY_LENGTH;

	return idx;
}

static int fill_get_statistics(private_context_t *ctx, uint8_t *buf, uint64_t id,
                               struct timespec *ts0, struct timespec *ts1)
{
	int idx = 0;

	/* pkt size */
	*((uint32_t *)&buf[idx]) =
	  conv_le32(ctx, PKT_GET_STATISTICS_LENGTH - PKT_SIZE_LENGTH);
	idx += PKT_SIZE_LENGTH;

	/* command */
	*((uint16_t *)&buf[idx]) = conv_le16(ctx, PKT_CMD_GET_STATISTICS);
	idx += PKT_CMD_TYPE_LENGTH;

	/* ID */
	*((uint64_t *)&buf[idx]) = conv_le64(ctx, id);
	idx += PKT_ID_LENGTH;

	/* ts0 */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, ts0->tv_sec);
	idx += PKT_SEC_LENGTH;

	*((uint32_t *)&buf[idx]) = conv_le32(ctx, ts0->tv_nsec);
	idx += PKT_NS_LENGTH;

	/* ts1 */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, ts1->tv_sec);
	idx += PKT_SEC_LENGTH;

	*((uint32_t *)&buf[idx]) = conv_le32(ctx, ts1->tv_nsec);
	idx += PKT_NS_LENGTH;

	return idx;
}

static history_gluon_result_t
parse_common_reply_header(private_context_t *ctx, uint8_t *buf,
                          length_check_func_t length_check_func,
                          uint32_t expected_length, int expected_pkt_type, int *index)
{
	int idx = 0;

	// Reply packet length
	uint32_t reply_length = restore_le32(ctx, &buf[idx]);
	idx += PKT_SIZE_LENGTH;
	if (length_check_func != NULL) {
		if((*length_check_func)() != HGL_SUCCESS) {
			ERR_MSG("reply length is not the expected: %d\n", reply_length);
			reset_context(ctx);
			return HGLERR_UNEXPECTED_REPLY_SIZE;
		}
	}
	else if (expected_length != LENGTH_CHECK_NONE &&
	         reply_length != expected_length) {
		ERR_MSG("reply length is not the expected: %d: %d",
		        reply_length, expected_length);
		reset_context(ctx);
		return HGLERR_UNEXPECTED_REPLY_SIZE;
	}

	// Reply type
	uint16_t reply_type = restore_le16(ctx, &buf[idx]);
	idx += PKT_CMD_TYPE_LENGTH;
	if (reply_type != expected_pkt_type) {
		ERR_MSG("reply type is not the expected: %d: %d\n",
		        reply_type, expected_pkt_type);
		return HGLERR_UNEXPECTED_REPLY_TYPE;
	}

	// result
	uint32_t result = restore_le32(ctx, &buf[idx]);
	idx += REPLY_RESULT_LENGTH;
	if (result != HGLSV_SUCCESS)
		return result;

	if (index)
		*index = idx;

	return HGL_SUCCESS;
}

static history_gluon_result_t
parse_reply_get_minimum_time(private_context_t *ctx, uint8_t *buf,
                             struct timespec *minimum_time)
{
	history_gluon_result_t ret;
	uint32_t expected_length = REPLY_GET_MIN_TIME_LENGTH - PKT_SIZE_LENGTH;
	int idx;
	ret = parse_common_reply_header(ctx, buf, NULL, expected_length,
	                                PKT_CMD_GET_MINIMUM_TIME, &idx);
	RETURN_IF_ERROR(ret);

	// sec
	uint32_t min_sec = restore_le32(ctx, &buf[idx]);
	idx += PKT_SEC_LENGTH;
	minimum_time->tv_sec = min_sec;

	// ns
	uint32_t min_ns = restore_le32(ctx, &buf[idx]);
	idx += PKT_NS_LENGTH;
	minimum_time->tv_nsec = min_ns;

	return HGL_SUCCESS;;
}

static history_gluon_result_t
parse_reply_delete(private_context_t *ctx, uint8_t *buf, uint64_t *num_deleted)
{
	history_gluon_result_t ret;
	uint32_t expected_length = REPLY_DELETE_LENGTH - PKT_SIZE_LENGTH;
	int idx;
	ret = parse_common_reply_header(ctx, buf, NULL, expected_length,
	                                PKT_CMD_DELETE, &idx);
	RETURN_IF_ERROR(ret);

	// Number of the deleted
	if (!num_deleted)
		return HGL_SUCCESS;
	*num_deleted = restore_le64(ctx, &buf[idx]);
	idx += REPLY_COUNT_DELETED_LENGTH;

	return HGL_SUCCESS;
}

static history_gluon_result_t
parse_reply_add(private_context_t *ctx, uint8_t *buf)
{
	history_gluon_result_t ret;
	uint32_t expected_length = REPLY_ADD_LENGTH - PKT_SIZE_LENGTH;
	int idx;
	ret = parse_common_reply_header(ctx, buf, NULL, expected_length,
	                                PKT_CMD_ADD_DATA, &idx);
	RETURN_IF_ERROR(ret);
	return HGL_SUCCESS;
}

static history_gluon_result_t
wait_and_check_add_result(private_context_t *ctx)
{
	history_gluon_result_t ret;
	uint8_t reply[REPLY_ADD_LENGTH];
	ret = read_data(ctx, reply, REPLY_ADD_LENGTH);
	RETURN_IF_ERROR(ret);
	return parse_reply_add(ctx, reply);
}

static history_gluon_result_t
parse_data_header(private_context_t *ctx, uint8_t *data_header,
                  history_gluon_data_t *gluon_data)
{
	int idx = 0;
	gluon_data->id = restore_le64(ctx, &data_header[idx]);
	idx += PKT_ID_LENGTH;

	gluon_data->ts.tv_sec = restore_le32(ctx, &data_header[idx]);
	idx += PKT_SEC_LENGTH;

	gluon_data->ts.tv_nsec = restore_le32(ctx, &data_header[idx]);
	idx += PKT_NS_LENGTH;

	gluon_data->type = restore_le16(ctx, &data_header[idx]);
	idx += PKT_DATA_TYPE_LENGTH;

	return HGL_SUCCESS;
}

static void init_gluon_data(history_gluon_data_t *gluon_data)
{
	gluon_data->id = -1;
	gluon_data->ts.tv_sec = 0;
	gluon_data->ts.tv_nsec = 0;
	gluon_data->type = HISTORY_GLUON_TYPE_INIT;
	gluon_data->length = 0;
}

static history_gluon_result_t
read_gluon_data_body_float(private_context_t *ctx,
                                      history_gluon_data_t *gluon_data)
{
	history_gluon_result_t ret;
	uint8_t buf[PKT_DATA_FLOAT_LENGTH];
	ret = read_data(ctx, buf, PKT_DATA_FLOAT_LENGTH);
	RETURN_IF_ERROR(ret);
	gluon_data->v.fp = read_ieee754_double(ctx, buf);
	return HGL_SUCCESS;
}

static history_gluon_result_t
read_gluon_data_body_string(private_context_t *ctx, history_gluon_data_t *gluon_data)
{
	history_gluon_result_t ret;

	/* read body size */
	uint8_t buf[PKT_DATA_STRING_SIZE_LENGTH];
	ret = read_data(ctx, buf, PKT_DATA_STRING_SIZE_LENGTH);
	RETURN_IF_ERROR(ret);
	gluon_data->length = restore_le32(ctx, buf);

	/* allocate body region */
	uint64_t alloc_size = gluon_data->length + 1;
	gluon_data->v.string = malloc(alloc_size);
	if (!gluon_data->v.string) {
		ERR_MSG("Failed to allocate: %" PRIu64 "\n", alloc_size);
		return HGLERR_MEM_ALLOC;
	}

	/* read body */
	ret = read_data(ctx, (uint8_t *)gluon_data->v.string,
	                gluon_data->length);
	RETURN_IF_ERROR(ret);
	gluon_data->v.string[gluon_data->length] = '\0';

	return HGL_SUCCESS;
}

static history_gluon_result_t
read_gluon_data_body_uint(private_context_t *ctx, history_gluon_data_t *gluon_data)
{
	history_gluon_result_t ret;
	uint8_t buf[PKT_DATA_UINT_LENGTH];
	ret = read_data(ctx, buf, PKT_DATA_UINT_LENGTH);
	RETURN_IF_ERROR(ret);
	gluon_data->v.uint = restore_le64(ctx, buf);
	return HGL_SUCCESS;
}

static history_gluon_result_t
read_gluon_data_body_blob(private_context_t *ctx, history_gluon_data_t *gluon_data)
{
	history_gluon_result_t ret;

	/* read body size */
	uint8_t buf[PKT_DATA_BLOB_SIZE_LENGTH];
	ret = read_data(ctx, buf, PKT_DATA_BLOB_SIZE_LENGTH);
	RETURN_IF_ERROR(ret);
	gluon_data->length = restore_le64(ctx, buf);

	/* allocate body region */
	gluon_data->v.blob = malloc(gluon_data->length);
	if (!gluon_data->v.blob) {
		ERR_MSG("Failed to allocate: %" PRIu64 "\n",
		        gluon_data->length);
		return HGLERR_MEM_ALLOC;
	}

	/* read body */
	ret = read_data(ctx, gluon_data->v.blob, gluon_data->length);
	RETURN_IF_ERROR(ret);

	return HGL_SUCCESS;
}

static history_gluon_result_t
read_gluon_data(private_context_t *ctx, history_gluon_data_t **gluon_data)
{
	history_gluon_result_t ret;

	/* allocate history_gluon_data_t variable */
	history_gluon_data_t *data = malloc(sizeof(history_gluon_data_t));
	if (!data)
		return HGLERR_MEM_ALLOC;
	init_gluon_data(data);

	/* read header */
	int data_header_size = PKT_ID_LENGTH +
	                       PKT_SEC_LENGTH + PKT_NS_LENGTH +
	                       PKT_DATA_TYPE_LENGTH;
	uint8_t data_header[data_header_size];
	ret = read_data(ctx, data_header, data_header_size);
	GOTO_IF_ERROR(ret, error);

	/* parse header */
	ret = parse_data_header(ctx, data_header, data);
	GOTO_IF_ERROR(ret, error);

	/* read data body */
	if (data->type == HISTORY_GLUON_TYPE_FLOAT)
		ret = read_gluon_data_body_float(ctx, data);
	else if (data->type == HISTORY_GLUON_TYPE_STRING)
		ret = read_gluon_data_body_string(ctx, data);
	else if (data->type == HISTORY_GLUON_TYPE_UINT)
		ret = read_gluon_data_body_uint(ctx, data);
	else if (data->type == HISTORY_GLUON_TYPE_BLOB)
		ret = read_gluon_data_body_blob(ctx, data);
	else {
		ERR_MSG("Unknown data type: %d", data->type);
		ret = HGLERR_INVALID_DATA_TYPE;
		goto error;
	}
	GOTO_IF_ERROR(ret, error);

	*gluon_data = data;
	return HGL_SUCCESS;

error:
	history_gluon_free_data(ctx, data);
	return ret;
}

/* ---------------------------------------------------------------------------
 * Public functions                                                           *
 * ------------------------------------------------------------------------- */
history_gluon_result_t
history_gluon_create_context(const char *database_name, const char *server_name,
                             int port, history_gluon_context_t *context)
{
	history_gluon_result_t ret;
	ret = check_database_name(database_name);
	if (ret != HGL_SUCCESS)
		return ret;

	private_context_t *ctx = malloc(sizeof(private_context_t));
	ctx->connected = 0;
	ctx->db_name = strdup(database_name);
	if (server_name)
		ctx->server_name = strdup(server_name);
	else
		ctx->server_name = strdup(DEFAULT_SERVER_NAME);
	if (port > 0)
		ctx->port = port;
	else
		ctx->port = DEFAULT_PORT;
	ctx->socket = -1;
	ctx->endian = get_system_endian();
	*context = ctx;
	return HGL_SUCCESS;
}

void history_gluon_free_context(history_gluon_context_t _ctx)
{
	private_context_t *ctx = (private_context_t *)_ctx;
	if (ctx->connected)
		close(ctx->socket);
	if (ctx->db_name)
		free(ctx->db_name);
	if (ctx->server_name)
		free(ctx->server_name);
	free(ctx);
}

history_gluon_result_t
history_gluon_add_float(history_gluon_context_t _ctx,
                        uint64_t id, struct timespec *ts, double data)
{
	private_context_t *ctx;
	history_gluon_result_t ret = get_connected_private_context(_ctx, &ctx);
	RETURN_IF_ERROR(ret);

	int pkt_size = PKT_ADD_DATA_HEADER_LENGTH + PKT_DATA_FLOAT_LENGTH;
	uint8_t buf[pkt_size];
	uint8_t *ptr = buf;

	/* header */
	ptr += fill_add_data_header(ctx, id, ts, ptr, HISTORY_GLUON_TYPE_FLOAT, pkt_size);

	/* data */
	write_ieee754_double(ctx, ptr, data);

	/* write data */
	ret = write_data(ctx, buf, pkt_size);
	RETURN_IF_ERROR(ret);

	/* check result */
	return wait_and_check_add_result(ctx);
}

history_gluon_result_t
history_gluon_add_uint(history_gluon_context_t _ctx,
                       uint64_t id, struct timespec *ts, uint64_t data)
{
	private_context_t *ctx;
	history_gluon_result_t ret = get_connected_private_context(_ctx, &ctx);
	RETURN_IF_ERROR(ret);

	int pkt_size = PKT_ADD_DATA_HEADER_LENGTH + PKT_DATA_UINT_LENGTH;
	uint8_t buf[pkt_size];
	uint8_t *ptr = buf;

	/* header */
	ptr += fill_add_data_header(ctx, id, ts, ptr, HISTORY_GLUON_TYPE_UINT, pkt_size);

	/* data */
	*((uint64_t *)ptr) = conv_le64(ctx, data);

	/* write data */
	ret = write_data(ctx, buf, pkt_size);
	RETURN_IF_ERROR(ret);

	/* check result */
	return wait_and_check_add_result(ctx);
}

history_gluon_result_t
history_gluon_add_string(history_gluon_context_t _ctx,
                         uint64_t id, struct timespec *ts, char *data)
{
	private_context_t *ctx;
	history_gluon_result_t ret = get_connected_private_context(_ctx, &ctx);
	RETURN_IF_ERROR(ret);

	if (!data) {
		ERR_MSG("data: NULL. id: %" PRIu64 ", ts: %u.%09u\n",
		        id, ts->tv_sec, ts->tv_nsec);
		return HGLERR_NULL_DATA;
	}

	uint32_t len_string = strlen(data);
	if (len_string > MAX_STRING_LENGTH) {
		ERR_MSG("string length is too long: %u. "
		        "id: %" PRIu64 ", ts:%u.%09u\n",
		        len_string, id, ts->tv_sec, ts->tv_nsec);
		return HGLERR_TOO_LONG_STRING;
	}
	uint32_t pkt_size = PKT_ADD_DATA_HEADER_LENGTH + PKT_DATA_STRING_SIZE_LENGTH;
	uint8_t buf[pkt_size];
	uint8_t *ptr = buf;

	/* header */
	ptr += fill_add_data_header(ctx, id, ts, ptr, HISTORY_GLUON_TYPE_STRING, pkt_size);

	/* length */
	*((uint32_t *)ptr) = conv_le32(ctx, len_string);
	ptr += PKT_DATA_STRING_SIZE_LENGTH;

	/* write header */
	ret = write_data(ctx, buf, pkt_size);
	if (ret < 0)
		return ret;

	/* write string body */
	ret = write_data(ctx, (uint8_t*)data, len_string);
	if (ret < 0)
		return ret;

	/* check result */
	return wait_and_check_add_result(ctx);
}

history_gluon_result_t
history_gluon_add_blob(history_gluon_context_t _ctx, uint64_t id, struct timespec *ts,
                       uint8_t *data, uint64_t length)
{
	private_context_t *ctx;
	history_gluon_result_t ret = get_connected_private_context(_ctx, &ctx);
	RETURN_IF_ERROR(ret);

	if (length > MAX_BLOB_LENGTH) {
		ERR_MSG("blob length is too long: %" PRIu64 "\n", length);
		return HGLERR_TOO_LARGE_BLOB;
	}
	uint32_t pkt_size = PKT_ADD_DATA_HEADER_LENGTH + PKT_DATA_BLOB_SIZE_LENGTH;
	uint8_t buf[pkt_size];
	uint8_t *ptr = buf;

	/* header */
	ptr += fill_add_data_header(ctx, id, ts, ptr, HISTORY_GLUON_TYPE_BLOB, pkt_size);

	/* length */
	*((uint64_t *)ptr) = conv_le64(ctx, length);
	ptr += PKT_DATA_BLOB_SIZE_LENGTH;

	/* write header */
	ret = write_data(ctx, buf, pkt_size);
	RETURN_IF_ERROR(ret);

	/* write string body */
	ret = write_data(ctx, data, length);
	RETURN_IF_ERROR(ret);

	/* check result */
	return wait_and_check_add_result(ctx);
}

history_gluon_result_t
history_gluon_query(history_gluon_context_t _ctx, uint64_t id, struct timespec *ts,
                    history_gluon_query_t query_type, history_gluon_data_t **gluon_data)
{
	private_context_t *ctx;
	history_gluon_result_t ret = get_connected_private_context(_ctx, &ctx);
	RETURN_IF_ERROR(ret);

	/* make a request packet and write it */
	uint8_t request[PKT_QUERY_DATA_LENGTH];
	int cmd_length = fill_query_data_header(ctx, id, ts, request, query_type);
	ret = write_data(ctx, request,cmd_length);
	RETURN_IF_ERROR(ret);

	/* reply */
	uint8_t reply[REPLY_QUERY_DATA_HEADER_LENGTH];
	ret = read_data(ctx, reply, REPLY_QUERY_DATA_HEADER_LENGTH);
	RETURN_IF_ERROR(ret);

	int idx;
	uint32_t expect_len = REPLY_QUERY_DATA_HEADER_LENGTH - PKT_SIZE_LENGTH;
	ret = parse_common_reply_header(ctx, reply, NULL, expect_len,
	                                PKT_CMD_QUERY_DATA, &idx);
	RETURN_IF_ERROR(ret);

	/* read data */
	ret = read_gluon_data(ctx, gluon_data);
	RETURN_IF_ERROR(ret);

	return HGL_SUCCESS;
}

void history_gluon_free_data(history_gluon_context_t _ctx,
                              history_gluon_data_t *gluon_data)
{
	if (gluon_data->type == HISTORY_GLUON_TYPE_STRING)
		free(gluon_data->v.string);
	else if (gluon_data->type == HISTORY_GLUON_TYPE_BLOB)
		free(gluon_data->v.blob);
	free(gluon_data);
}

history_gluon_result_t
history_gluon_query_all(history_gluon_context_t _ctx,
                        history_gluon_stream_event_cb_func event_cb,
                        void *priv_data)
{
	if (!event_cb) {
		ERR_MSG("event_cb is NULL\n");
		return HGLERR_INVALID_PARAMETER;
	}

	private_context_t *ctx;
	history_gluon_result_t ret = get_connected_private_context(_ctx, &ctx);
	RETURN_IF_ERROR(ret);

	/* request */
	uint8_t request[PKT_QUERY_ALL_LENGTH];
	int cmd_length = fill_query_all_packet(ctx, request);
	ret = write_data(ctx, request, cmd_length);
	RETURN_IF_ERROR(ret);

	/* reply */
	uint8_t reply[REPLY_QUERY_ALL_LENGTH];
	ret = read_data(ctx, reply, REPLY_QUERY_ALL_LENGTH);
	RETURN_IF_ERROR(ret);
	uint16_t reply_type = restore_le16(ctx, &reply[PKT_QUERY_ALL_LENGTH]);
	if (reply_type != PKT_CMD_QUERY_ALL) {
		ERR_MSG("reply type is not the expected: %d: %d\n",
		        reply_type, PKT_CMD_QUERY_ALL);
		return HGLERR_UNEXPECTED_REPLY_TYPE;
	}

	/* loop to send obtained data */
	history_gluon_stream_event_t evt;
	while (1) {
		history_gluon_data_t *gluon_data;
		ret = read_gluon_data(ctx, &gluon_data);
		BREAK_IF_ERROR(ret);

		/* make an argument for callback function */
		ret = read_gluon_data(ctx, &evt.data);
		BREAK_IF_ERROR(ret);

		evt.type = HISTORY_GLUON_STREAM_EVENT_GOT_DATA;
		evt.priv_data = priv_data;
		evt.flags = 0;

		/* execute the callback function */
		(*event_cb)(&evt);
	}

	/* callback to tell the end. */
	evt.type = HISTORY_GLUON_STREAM_EVENT_END;
	evt.data = NULL;
	evt.priv_data = priv_data;
	evt.flags = 0;
	(*event_cb)(&evt);

	return ret;
}

history_gluon_result_t
history_gluon_range_query(history_gluon_context_t _ctx, uint64_t id,
                          struct timespec *ts0, struct timespec *ts1,
                          history_gluon_sort_order_t sort_request,
                          uint64_t num_max_entries,
                          history_gluon_data_array_t **array)
{
	private_context_t *ctx;
	history_gluon_result_t ret = get_connected_private_context(_ctx, &ctx);
	RETURN_IF_ERROR(ret);

	/* make a request packet and write it */
	uint8_t request[PKT_RANGE_QUERY_LENGTH];
	int cmd_length = fill_range_query_header(ctx, request, id, ts0, ts1,
	                                         sort_request, num_max_entries);
	ret = write_data(ctx, request, cmd_length);
	RETURN_IF_ERROR(ret);

	/* reply */
	uint8_t reply[REPLY_RANGE_QUERY_HEADER_LENGTH];
	ret = read_data(ctx, reply, REPLY_RANGE_QUERY_HEADER_LENGTH);
	RETURN_IF_ERROR(ret);

	int idx;
	uint32_t expect_len = REPLY_RANGE_QUERY_HEADER_LENGTH - PKT_SIZE_LENGTH;
	ret = parse_common_reply_header(ctx, reply, NULL, expect_len,
	                                PKT_CMD_RANGE_QUERY, &idx);
	RETURN_IF_ERROR(ret);

	/* number of entries */
	uint64_t num_data = restore_le64(ctx, &reply[idx]);
	idx += PKT_NUM_ENTRIES_LENGTH;

	/* sort order */
	history_gluon_sort_order_t actual_sort_order;
	actual_sort_order = restore_le16(ctx, &reply[idx]);
	idx += PKT_SORT_ORDER_LENGTH;

	/* allocate data array */
	int alloc_size = sizeof(history_gluon_data_array_t);
	history_gluon_data_array_t *arr = malloc(alloc_size);
	if (!arr) {
		ERR_MSG("Failed to allocate: %" PRIu64 "\n", alloc_size);
		return HGLERR_MEM_ALLOC;
	}
	arr->num_data = 0;
	arr->sort_order = actual_sort_order;

	alloc_size = sizeof(history_gluon_data_t *) * num_data;
	arr->array = malloc(alloc_size);
	if (!arr->array) {
		ret = HGLERR_MEM_ALLOC;
		ERR_MSG("Failed to allocate: %" PRIu64 "\n", alloc_size);
		goto error;
	}

	/* data */
	uint64_t i;
	for (i = 0; i < num_data; i++) {
		history_gluon_data_t *gluon_data;
		ret = read_gluon_data(ctx, &gluon_data);
		GOTO_IF_ERROR(ret, error);
		arr->array[i] = gluon_data;
		arr->num_data++;
	}

	*array = arr;
	return HGL_SUCCESS;

error:
	history_gluon_free_data_array(ctx, arr);
	return ret;
}

void history_gluon_free_data_array(history_gluon_context_t _ctx,
                                    history_gluon_data_array_t *array)
{
	uint64_t i = 0;
	for (i = 0; i < array->num_data; i++) {
		history_gluon_data_t *data = array->array[i];
		history_gluon_free_data(_ctx, data);
	}
	if (array->array)
		free(array->array);
	free(array);
}

history_gluon_result_t
history_gluon_get_minimum_time(history_gluon_context_t _ctx,
                               uint64_t id, struct timespec *minimum_ts)
{
	private_context_t *ctx;
	history_gluon_result_t ret = get_connected_private_context(_ctx, &ctx);
	RETURN_IF_ERROR(ret);

	// request
	uint8_t request[PKT_GET_MIN_TIME_LENGTH];
	int cmd_length = fill_get_minimum_time_packet(ctx, request, id);
	ret = write_data(ctx, request, cmd_length);
	RETURN_IF_ERROR(ret);

	// reply
	uint8_t reply[REPLY_GET_MIN_TIME_LENGTH];
	ret = read_data(ctx, reply, REPLY_GET_MIN_TIME_LENGTH);
	RETURN_IF_ERROR(ret);

	ret = parse_reply_get_minimum_time(ctx, reply, minimum_ts);
	RETURN_IF_ERROR(ret);

	return HGL_SUCCESS;
}

history_gluon_result_t
history_gluon_get_statistics(history_gluon_context_t _ctx, uint64_t id,
                             struct timespec *ts0, struct timespec *ts1,
                             history_gluon_statistics_t *statistics)
{
	private_context_t *ctx;
	history_gluon_result_t ret = get_connected_private_context(_ctx, &ctx);
	RETURN_IF_ERROR(ret);

	// request
	uint8_t request[PKT_GET_STATISTICS_LENGTH];
	int cmd_length = fill_get_statistics(ctx, request, id, ts0, ts1);
	ret = write_data(ctx, request, cmd_length);
	RETURN_IF_ERROR(ret);

	// read reply packet
	uint8_t reply[REPLY_STATISTICS_LENGTH];
	ret = read_data(ctx, reply, REPLY_STATISTICS_LENGTH);
	RETURN_IF_ERROR(ret);

	uint32_t expected_length = REPLY_STATISTICS_LENGTH - PKT_SIZE_LENGTH;
	int idx;
	ret = parse_common_reply_header(ctx, reply, NULL, expected_length,
	                                PKT_CMD_GET_STATISTICS, &idx);
	RETURN_IF_ERROR(ret);

	// item ID
	statistics->id = restore_le64(ctx, &reply[idx]);
	idx += PKT_ID_LENGTH;

	// count
	statistics->count = restore_le64(ctx, &reply[idx]);
	idx += PKT_DATA_UINT_LENGTH;

	// min and max
	statistics->min = read_ieee754_double(ctx, &reply[idx]);
	idx += PKT_DATA_FLOAT_LENGTH;
	statistics->max = read_ieee754_double(ctx, &reply[idx]);
	idx += PKT_DATA_FLOAT_LENGTH;

	// sum
	statistics->sum = read_ieee754_double(ctx, &reply[idx]);
	idx += PKT_DATA_FLOAT_LENGTH;

	// calculate average and delta
	statistics->average = statistics->sum / statistics->count;
	statistics->delta = statistics->max - statistics->min;

	return HGL_SUCCESS;
}

history_gluon_result_t
history_gluon_delete(history_gluon_context_t _ctx, uint64_t id, struct timespec *ts,
                     history_gluon_delete_way_t delete_way, uint64_t *num_deleted_entries)
{
	private_context_t *ctx;
	history_gluon_result_t ret = get_connected_private_context(_ctx, &ctx);
	RETURN_IF_ERROR(ret);

	// request
	uint8_t request[PKT_DELETE_LENGTH];
	int cmd_length = fill_delete_packet(ctx, request, id, ts, delete_way);
	ret = write_data(ctx, request, cmd_length);
	RETURN_IF_ERROR(ret);

	// reply
	uint8_t reply[REPLY_DELETE_LENGTH];
	ret = read_data(ctx, reply, REPLY_DELETE_LENGTH);
	RETURN_IF_ERROR(ret);

	ret = parse_reply_delete(ctx, reply, num_deleted_entries);
	RETURN_IF_ERROR(ret);
	return HGL_SUCCESS;
}

