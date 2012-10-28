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
#define PKT_SEARCH_WHEN_NOT_FOUND_LENGTH 2

#define PKT_QUERY_DATA_LENGTH \
(PKT_SIZE_LENGTH + \
 PKT_CMD_TYPE_LENGTH + \
 PKT_ID_LENGTH + \
 PKT_SEC_LENGTH + \
 PKT_NS_LENGTH + \
 PKT_SEARCH_WHEN_NOT_FOUND_LENGTH)

#define REPLY_QUERY_DATA_FOUND_FLAG_LENGTH 2

#define REPLY_QUERY_DATA_HEADER_LENGTH \
(PKT_SIZE_LENGTH + \
 PKT_CMD_TYPE_LENGTH + \
 REPLY_RESULT_LENGTH + \
 REPLY_QUERY_DATA_FOUND_FLAG_LENGTH)

/* Range Query */
#define PKT_NUM_ENTRIES_LENGTH 4
#define PKT_SORT_ORDER_LENGTH  2

#define PKT_RANGE_QUERY_LENGTH \
(PKT_SIZE_LENGTH + \
 PKT_CMD_TYPE_LENGTH + \
 PKT_ID_LENGTH + \
 PKT_SEC_LENGTH * 2 + \
 PKT_NUM_ENTRIES_LENGTH + \
 PKT_DATA_ORDER_LENGTH)

/* Get Minimum Time */
#define PKT_GET_MIN_TIME_LENGTH \
(PKT_SIZE_LENGTH + \
 PKT_CMD_TYPE_LENGTH + \
 PKT_ID_LENGTH)

#define REPLY_GET_MIN_TIME_LENGTH \
(PKT_SIZE_LENGTH + \
 PKT_CMD_TYPE_LENGTH + \
 REPLY_RESULT_LENGTH + \
 PKT_SEC_LENGTH)

/* Get Statistics */
#define PKT_GET_STATISTICS_LENGTH \
(PKT_SIZE_LENGTH + \
 PKT_CMD_TYPE_LENGTH + \
 PKT_ID_LENGTH + \
 PKT_SEC_LENGTH*2)

#define REPLY_STATISTICS_LENGTH \
(PKT_SIZE_LENGTH + \
 PKT_CMD_TYPE_LENGTH + \
 REPLY_RESULT_LENGTH + \
 PKT_ID_LENGTH + \
 PKT_SEC_LENGTH*2 + \
 PKT_DATA_UINT_LENGTH + \
 PKT_DATA_FLOAT_LENGTH*3)

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
	PKT_CMD_GET                = 1000,
	PKT_CMD_GET_MIN_SEC        = 1100,
	PKT_CMD_GET_STATISTICS     = 1200,
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
}
private_context_t;

typedef int (*length_check_func_t)(void);
#define LENGTH_CHECK_NONE 0

/* ---------------------------------------------------------------------------
 * Private methods
 * ------------------------------------------------------------------------- */
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

static void reset_context(private_context_t *ctx)
{
	if (ctx->socket != -1) {
		close(ctx->socket);
		ctx->socket = -1;
	}
	ctx->connected = 0;
}

static int connect_to_history_service(private_context_t *ctx)
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
		return -1;
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
		return -1;
	}

	ctx->connected = 1;
	ctx->socket = sock;
	INFO("Connected to history server: host: %s, port: %s\n",
	     ctx->server_name, port_str);

	return 0;
}

static private_context_t *get_connected_private_context(history_gluon_context_t ctx)
{
	private_context_t *priv_ctx = ctx;
	if (!priv_ctx->connected) {
		if (connect_to_history_service(priv_ctx) == -1)
			return NULL;
	}
	return priv_ctx;
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
	idx += PKT_SORT_ORDER_LENGTH;

	return idx;
}

static int fill_get_min_sec_packet(private_context_t *ctx, uint8_t *buf, uint64_t id)
{
	int idx = 0;

	/* pkt size */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, PKT_GET_MIN_TIME_LENGTH - PKT_SIZE_LENGTH);
	idx += PKT_SIZE_LENGTH;

	/* command */
	*((uint16_t *)&buf[idx]) = conv_le16(ctx, PKT_CMD_GET_MIN_SEC);
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
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, PKT_GET_STATISTICS_LENGTH - PKT_SIZE_LENGTH);
	idx += PKT_SIZE_LENGTH;

	/* command */
	*((uint16_t *)&buf[idx]) = conv_le16(ctx, PKT_CMD_GET_STATISTICS);
	idx += PKT_CMD_TYPE_LENGTH;

	/* ID */
	*((uint64_t *)&buf[idx]) = conv_le64(ctx, id);
	idx += PKT_ID_LENGTH;

	/* sec0 */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, ts0->tv_sec);
	idx += PKT_SEC_LENGTH;

	/* sec1 */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, ts1->tv_sec);
	idx += PKT_SEC_LENGTH;

	return idx;
}

static int write_data(private_context_t *ctx, uint8_t *buf, uint64_t count)
{
	uint8_t *ptr = buf;
	while (count > 0) {
		ssize_t written_byte = write(ctx->socket, ptr, count);
		if (written_byte == -1) {
			ERR_MSG("Failed to write data: %d\n", errno);
			reset_context(ctx);
			return -1;
		}
		count -= written_byte;
		ptr += written_byte;
	}
	return 0;
}

static int read_data(private_context_t *ctx, uint8_t *buf, uint64_t count)
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

static int parse_common_reply_header
  (private_context_t *ctx, uint8_t *buf,
   length_check_func_t length_check_func, uint32_t expected_length,
   int expected_pkt_type)
{
	int idx = 0;

	// Reply packet length
	uint32_t reply_length = restore_le32(ctx, &buf[idx]);
	idx += PKT_SIZE_LENGTH;
	if (length_check_func != NULL) {
		if((*length_check_func)() == -1) {
			ERR_MSG("reply length is not the expected: %d\n", reply_length);
			reset_context(ctx);
			return -1;
		}
	}
	else if (expected_length != LENGTH_CHECK_NONE &&
	         reply_length != expected_length) {
		ERR_MSG("reply length is not the expected: %d: %d",
		        reply_length, expected_length);
		reset_context(ctx);
		return -1;
	}

	// Reply type
	uint16_t reply_type = restore_le16(ctx, &buf[idx]);
	idx += PKT_CMD_TYPE_LENGTH;
	if (reply_type != expected_pkt_type) {
		ERR_MSG("reply type is not PKT_CMD_DELETE: %d: %d\n",
		        reply_type, PKT_CMD_DELETE);
		return -1;
	}

	// result
	uint32_t result = restore_le32(ctx, &buf[idx]);
	idx += REPLY_RESULT_LENGTH;
	if (result != RESULT_SUCCESS) {
		ERR_MSG("result is not Success: %d\n", result);
		return -1;
	}

	return idx;
}

static int parse_reply_get_min_sec(private_context_t *ctx, uint8_t *buf,
                                     struct timespec *minimum_time)
{
	uint32_t expected_length = REPLY_GET_MIN_TIME_LENGTH - PKT_SIZE_LENGTH;
	int idx = parse_common_reply_header(ctx, buf, NULL, expected_length,
	                                    PKT_CMD_GET_MIN_SEC);
	if (idx == -1)
		return -1;

	// min_sec
	uint32_t _min_sec = restore_le32(ctx, &buf[idx]);
	idx += PKT_SEC_LENGTH;
	minimum_time->tv_sec = _min_sec;
	minimum_time->tv_nsec = 0;

	return 0;
}

static uint64_t parse_reply_delete(private_context_t *ctx, uint8_t *buf)
{
	uint32_t expected_length = REPLY_DELETE_LENGTH - PKT_SIZE_LENGTH;
	int idx = parse_common_reply_header(ctx, buf, NULL, expected_length,
	                                    PKT_CMD_DELETE);
	if (idx == -1)
		return -1;

	// Number of the deleted
	uint64_t num_deleted = restore_le64(ctx, &buf[idx]);
	idx += REPLY_COUNT_DELETED_LENGTH;

	return num_deleted;
}

static int parse_reply_add(private_context_t *ctx, uint8_t *buf)
{
	uint32_t expected_length = REPLY_ADD_LENGTH - PKT_SIZE_LENGTH;
	int idx = parse_common_reply_header(ctx, buf, NULL, expected_length,
	                                    PKT_CMD_ADD_DATA);
	if (idx == -1)
		return -1;
	return 0;
}

static int wait_and_check_add_result(private_context_t *ctx)
{
	uint8_t reply[REPLY_ADD_LENGTH];
	if (read_data(ctx, reply, REPLY_ADD_LENGTH) == -1)
		return -1;
	return parse_reply_add(ctx, reply);
}

static int parse_data_header(private_context_t *ctx,
                             uint8_t *data_header,

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

static int read_gluon_data_body_float(private_context_t *ctx,
                                      history_gluon_data_t *gluon_data)
{
	uint8_t buf[PKT_DATA_FLOAT_LENGTH];
	int ret = read_data(ctx, buf, PKT_DATA_FLOAT_LENGTH);
	if (ret < 0)
		return ret;
	gluon_data->v_float = read_ieee754_double(ctx, buf);
	return HGL_SUCCESS;
}

static int read_gluon_data_body_string(private_context_t *ctx,
                                       history_gluon_data_t *gluon_data)
{
	/* read body size */
	uint8_t buf[PKT_DATA_STRING_SIZE_LENGTH];
	int ret = read_data(ctx, buf, PKT_DATA_STRING_SIZE_LENGTH);
	if (ret < 0)
		return ret;
	gluon_data->length = restore_le64(ctx, buf);

	/* allocate body region */
	uint64_t alloc_size = gluon_data->length + 1;
	gluon_data->v_string = malloc(alloc_size);
	if (!gluon_data->v_string) {
		ERR_MSG("Failed to allocate: %" PRIu64 "\n", alloc_size);
		return HGLERR_MEM_ALLOC;
	}

	/* read body */
	ret = read_data(ctx, (uint8_t *)gluon_data->v_string,
	                gluon_data->length);
	if (ret < 0)
		return ret;
	gluon_data->v_string[alloc_size] = '\0';

	return HGL_SUCCESS;
}

static int read_gluon_data_body_uint(private_context_t *ctx,
                                     history_gluon_data_t *gluon_data)
{
	uint8_t buf[PKT_DATA_UINT_LENGTH];
	int ret = read_data(ctx, buf, PKT_DATA_UINT_LENGTH);
	if (ret < 0)
		return ret;
	gluon_data->v_uint = restore_le64(ctx, buf);
	return HGL_SUCCESS;
}

static int read_gluon_data_body_blob(private_context_t *ctx,
                                     history_gluon_data_t *gluon_data)
{
	/* read body size */
	uint8_t buf[PKT_DATA_BLOB_SIZE_LENGTH];
	int ret = read_data(ctx, buf, PKT_DATA_BLOB_SIZE_LENGTH);
	if (ret < 0)
		return ret;
	gluon_data->length = restore_le64(ctx, buf);

	/* allocate body region */
	gluon_data->v_blob = malloc(gluon_data->length);
	if (!gluon_data->v_blob) {
		ERR_MSG("Failed to allocate: %" PRIu64 "\n",
		        gluon_data->length);
		return HGLERR_MEM_ALLOC;
	}

	/* read body */
	ret = read_data(ctx, gluon_data->v_blob, gluon_data->length);
	if (ret < 0)
		return ret;

	return HGL_SUCCESS;
}

static int read_gluon_data(private_context_t *ctx,
                               history_gluon_data_t **gluon_data)
{
	/* allocate history_gluon_data_t variable */
	history_gluon_data_t *data = malloc(sizeof(history_gluon_data_t));
	if (!*gluon_data)
		return HGLERR_MEM_ALLOC;
	init_gluon_data(data);

	/* read header */
	int data_header_size = PKT_ID_LENGTH +
	                       PKT_SEC_LENGTH + PKT_NS_LENGTH +
	                       PKT_DATA_TYPE_LENGTH;
	uint8_t data_header[data_header_size];
	int ret = read_data(ctx, data_header, data_header_size);
	if (ret < 0)
		goto error;

	/* parse header */
	ret = parse_data_header(ctx, data_header, data);
	if (ret < 0)
		goto error;

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
	if (ret < 0)
		goto error;

	*gluon_data = data;
	return HGL_SUCCESS;

error:
	history_gluon_free_data(ctx, data);
	return ret;
}

/* ---------------------------------------------------------------------------
 * Public functions                                                           *
 * ------------------------------------------------------------------------- */
history_gluon_context_t history_gluon_create_context(void)
{
	private_context_t *ctx = malloc(sizeof(private_context_t));
	ctx->connected = 0;
	ctx->server_name = DEFAULT_SERVER_NAME;
	ctx->port = DEFAULT_PORT;
	ctx->socket = -1;
	ctx->endian = get_system_endian();
	return ctx;
}

void history_gluon_free_context(history_gluon_context_t _ctx)
{
	private_context_t *ctx = (private_context_t *)_ctx;
	if (ctx->connected)
		close(ctx->socket);
	free(ctx);
}

int history_gluon_add_float(history_gluon_context_t _ctx,
                            uint64_t id, struct timespec *ts, double data)
{
	private_context_t *ctx = get_connected_private_context(_ctx);
	if (ctx == NULL)
		return -1;

	int pkt_size = PKT_ADD_DATA_HEADER_LENGTH + PKT_DATA_FLOAT_LENGTH;
	uint8_t buf[pkt_size];
	uint8_t *ptr = buf;

	/* header */
	ptr += fill_add_data_header(ctx, id, ts, ptr,
	                            HISTORY_GLUON_TYPE_FLOAT, pkt_size);

	/* data */
	write_ieee754_double(ctx, ptr, data);

	/* write data */
	if (write_data(ctx, buf, pkt_size) == -1)
		return -1;

	/* check result */
	return wait_and_check_add_result(ctx);
}

int history_gluon_add_uint(history_gluon_context_t _ctx,
                           uint64_t id, struct timespec *ts, uint64_t data)
{
	private_context_t *ctx = get_connected_private_context(_ctx);
	if (ctx == NULL)
		return -1;

	int pkt_size = PKT_ADD_DATA_HEADER_LENGTH + PKT_DATA_UINT_LENGTH;
	uint8_t buf[pkt_size];
	uint8_t *ptr = buf;

	/* header */
	ptr += fill_add_data_header(ctx, id, ts, ptr,
	                            HISTORY_GLUON_TYPE_UINT, pkt_size);

	/* data */
	*((uint64_t *)ptr) = conv_le64(ctx, data);

	/* write data */
	if (write_data(ctx, buf, pkt_size) == -1)
		return -1;

	/* check result */
	return wait_and_check_add_result(ctx);
}

int history_gluon_add_string(history_gluon_context_t _ctx,
                             uint64_t id, struct timespec *ts, char *data)
{
	private_context_t *ctx = get_connected_private_context(_ctx);
	if (ctx == NULL)
		return -1;

	uint32_t len_string = strlen(data);
	if (len_string > MAX_STRING_LENGTH) {
		ERR_MSG("string length is too long: %d", len_string);
		return -1;
	}
	uint32_t pkt_size =
	  PKT_ADD_DATA_HEADER_LENGTH + PKT_DATA_STRING_SIZE_LENGTH;
	uint8_t *buf = malloc(pkt_size);
	if (!buf) {
		ERR_MSG("Failed to malloc: %d", pkt_size);
		return -1;
	}
	uint8_t *ptr = buf;

	/* header */
	ptr += fill_add_data_header(ctx, id, ts, ptr,
	                            HISTORY_GLUON_TYPE_STRING, pkt_size);

	/* length */
	*((uint32_t *)ptr) = conv_le32(ctx, len_string);
	ptr += PKT_DATA_STRING_SIZE_LENGTH;

	/* string body */
	memcpy(ptr, data, len_string);

	/* write header */
	int ret = write_data(ctx, buf, pkt_size);
	free(buf);
	if (ret == -1)
		return -1;

	/* write string body */
	if(write_data(ctx, (uint8_t*)data, len_string) == -1)
		return -1;

	/* check result */
	return wait_and_check_add_result(ctx);
}

int history_gluon_add_blob(history_gluon_context_t _ctx,
                           uint64_t id, struct timespec *ts, uint8_t *data,
                           uint64_t length)
{
	private_context_t *ctx = get_connected_private_context(_ctx);
	if (ctx == NULL)
		return -1;

	if (length > MAX_BLOB_LENGTH) {
		ERR_MSG("blob length is too long: %d", length);
		return -1;
	}
	uint32_t pkt_size =
	  PKT_ADD_DATA_HEADER_LENGTH + PKT_DATA_BLOB_SIZE_LENGTH;
	
	uint8_t *buf = malloc(pkt_size);
	if (!buf) {
		ERR_MSG("Failed to malloc: %d", pkt_size);
		return -1;
	}
	uint8_t *ptr = buf;

	/* header */
	ptr += fill_add_data_header(ctx, id, ts, ptr,
	                            HISTORY_GLUON_TYPE_BLOB, pkt_size);

	/* length */
	*((uint64_t *)ptr) = conv_le64(ctx, length);
	ptr += PKT_DATA_BLOB_SIZE_LENGTH;

	/* string body */
	memcpy(ptr, data, length);

	/* write header */
	int ret = write_data(ctx, buf, pkt_size);
	free(buf);
	if (ret == -1)
		return -1;

	/* write string body */
	if(write_data(ctx, data, length) == -1)
		return -1;

	/* check result */
	return wait_and_check_add_result(ctx);
}

int history_gluon_query(history_gluon_context_t _ctx,
                        uint64_t id, struct timespec *ts,
                        history_gluon_query_t query_type,
                        history_gluon_data_t **gluon_data)
{
	int ret;
	private_context_t *ctx = get_connected_private_context(_ctx);
	if (ctx == NULL)
		return -1;

	/* make a request packet and write it */
	uint8_t request[PKT_QUERY_DATA_LENGTH];
	fill_query_data_header(ctx, id, ts, request, query_type);
	ret = write_data(ctx, request, PKT_QUERY_DATA_LENGTH);
	if (ret < 0)
		return ret;

	/* reply */
	uint8_t reply[REPLY_QUERY_DATA_HEADER_LENGTH];
	ret = read_data(ctx, reply, REPLY_QUERY_DATA_HEADER_LENGTH);
	if (ret < 0)
		return ret;
	ret = parse_common_reply_header(ctx, reply, NULL,
	                                REPLY_QUERY_DATA_HEADER_LENGTH,
	                                PKT_CMD_QUERY_DATA);
	if (ret < 0)
		return ret;

	/* found flag */
	int idx = ret;
	uint16_t found = restore_le16(ctx, &reply[idx]);
	idx += REPLY_QUERY_DATA_FOUND_FLAG_LENGTH;
	if (found == 0)
		*gluon_data = NULL;

	ret = read_gluon_data(ctx, gluon_data);
	if (ret < 0)
		return ret;
	return HGL_SUCCESS;
}

void history_gluon_free_data(history_gluon_context_t _ctx,
                              history_gluon_data_t *gluon_data)
{
	if (gluon_data->type == HISTORY_GLUON_TYPE_STRING)
		free(gluon_data->v_string);
	else if (gluon_data->type == HISTORY_GLUON_TYPE_BLOB)
		free(gluon_data->v_blob);
	free(gluon_data);
}

int history_gluon_range_query(history_gluon_context_t _ctx, uint64_t id,
                              struct timespec *ts0,
                              struct timespec *ts1,
                              history_gluon_sort_order_t sort_request,
                              history_gluon_data_array_t **array)
{
	ERR_MSG("Not implemented yet\n");
	return -1;
}

void history_gluon_free_data_array(history_gluon_context_t _ctx,
                                    history_gluon_data_array_t *array)
{
	uint64_t i = 0;
	for (i = 0; i < array->num_data; i++) {
		history_gluon_data_t *data = &(array->array[i]);
		history_gluon_free_data(_ctx, data);
	}
}

int history_gluon_get_minmum_time(history_gluon_context_t _ctx,
                                  uint64_t id, struct timespec *minimum_ts)
{
	private_context_t *ctx = get_connected_private_context(_ctx);
	if (ctx == NULL)
		return -1;

	// request
	uint8_t request[PKT_GET_MIN_TIME_LENGTH];
	fill_get_min_sec_packet(ctx, request, id);
	if (write_data(ctx, request, PKT_GET_MIN_TIME_LENGTH) == -1)
		return -1;

	// reply
	uint8_t reply[REPLY_GET_MIN_TIME_LENGTH];
	if (read_data(ctx, reply, REPLY_GET_MIN_TIME_LENGTH) == -1)
		return -1;
	if (parse_reply_get_min_sec(ctx, reply, minimum_ts))
		return -1;
	return 0;
}

int history_gluon_get_statistics(history_gluon_context_t _ctx, uint64_t id,
                                 struct timespec *ts0, struct timespec *ts1,
                                 history_gluon_statistics_t *statistics)
{
	private_context_t *ctx = get_connected_private_context(_ctx);
	if (ctx == NULL)
		return -1;

	// request
	uint8_t request[PKT_GET_STATISTICS_LENGTH];
	int cmd_length = fill_get_statistics(ctx, request, id, ts0, ts1);
	if (write_data(ctx, request, cmd_length) == -1)
		return -1;

	// read reply packet
	uint8_t reply[REPLY_STATISTICS_LENGTH];
	if (read_data(ctx, reply, REPLY_STATISTICS_LENGTH) == -1)
		return -1;
	uint32_t expected_length = REPLY_STATISTICS_LENGTH - PKT_SIZE_LENGTH;
	int idx = parse_common_reply_header(ctx, reply, NULL, expected_length,
	                                    PKT_CMD_GET_STATISTICS);
	if (idx == -1)
		return -1;

	// item ID
	statistics->id = restore_le64(ctx, &reply[idx]);
	idx += PKT_ID_LENGTH;

	// sec0
	statistics->ts0.tv_sec = restore_le32(ctx, &reply[idx]);
	statistics->ts0.tv_nsec = 0;
	idx += PKT_SEC_LENGTH;

	// sec1
	statistics->ts1.tv_sec = restore_le32(ctx, &reply[idx]);
	statistics->ts1.tv_nsec = 0;
	idx += PKT_SEC_LENGTH;

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

	return 0;
}

int history_gluon_delete(history_gluon_context_t _ctx, uint64_t id,
                         struct timespec *ts,
                         history_gluon_delete_way_t delete_way,
                         uint64_t *num_deleted_entries)
{
	private_context_t *ctx = get_connected_private_context(_ctx);
	if (ctx == NULL)
		return -1;

	// request
	uint8_t request[PKT_DELETE_LENGTH];
	fill_delete_packet(ctx, request, id, ts, delete_way);
	if (write_data(ctx, request, PKT_DELETE_LENGTH) == -1)
		return -1;

	// reply
	uint8_t reply[REPLY_DELETE_LENGTH];
	if (read_data(ctx, reply, REPLY_DELETE_LENGTH) == -1)
		return -1;
	int num_deleted = parse_reply_delete(ctx, reply);
	if (num_deleted == -1)
		return -1;
	if (num_deleted_entries)
		*num_deleted_entries = num_deleted;
	return 0;
}

