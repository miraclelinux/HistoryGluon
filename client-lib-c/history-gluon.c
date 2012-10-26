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

#include "history-gluon.h"
#include "message.h"

#define DEFAULT_PORT 30010
#define DEFAULT_SERVER_NAME "localhost"

/* Common header */
#define PKT_SIZE_LENGTH           4
#define PKT_TYPE_LENGTH           2
#define PKT_DATA_TYPE_LENGTH      2
#define PKT_ITEM_ID_LENGTH        8
#define PKT_CLOCK_LENGTH          4
#define PKT_NS_LENGTH             4

/* Common reply */
#define REPLY_RESULT_LENGTH  4

/* Add packets */
#define PKT_DATA_FLOAT_LENGTH     8
#define PKT_DATA_UINT64_LENGTH    8
#define PKT_DATA_STRING_SIZE_LENGTH  4

#define PKT_ADD_DATA_HEADER_LENGTH \
(PKT_SIZE_LENGTH + PKT_TYPE_LENGTH + PKT_DATA_TYPE_LENGTH + PKT_ITEM_ID_LENGTH + PKT_CLOCK_LENGTH + PKT_NS_LENGTH)


/* Get data */
#define PKT_NUM_ENTRIES_LENGTH 4
#define PKT_DATA_ORDER_LENGTH  2

#define PKT_GET_DATA_LENGTH \
(PKT_SIZE_LENGTH + PKT_TYPE_LENGTH + PKT_ITEM_ID_LENGTH + PKT_CLOCK_LENGTH*2 + PKT_NUM_ENTRIES_LENGTH + PKT_DATA_ORDER_LENGTH)

/* Get data with timestamp */
#define PKT_SEARCH_NEAR_LENGTH 1
#define PKT_GET_DATA_WITH_TIMESTAMP_LENGTH \
(PKT_SIZE_LENGTH + PKT_TYPE_LENGTH + PKT_ITEM_ID_LENGTH + PKT_CLOCK_LENGTH + PKT_NS_LENGTH + PKT_SEARCH_NEAR_LENGTH)

/* Get Min clock packets */
#define PKT_GET_MIN_CLOCK_LENGTH \
(PKT_SIZE_LENGTH + PKT_TYPE_LENGTH + PKT_ITEM_ID_LENGTH)

#define REPLY_GET_MIN_CLOCK_LENGTH \
(PKT_SIZE_LENGTH + PKT_TYPE_LENGTH + REPLY_RESULT_LENGTH + PKT_CLOCK_LENGTH)

/* Get statistics */
#define PKT_GET_STATISTICS_LENGTH \
(PKT_SIZE_LENGTH + PKT_TYPE_LENGTH + PKT_ITEM_ID_LENGTH + PKT_CLOCK_LENGTH*2)

#define REPLY_STATISTICS_LENGTH \
(PKT_SIZE_LENGTH + PKT_TYPE_LENGTH + REPLY_RESULT_LENGTH + PKT_ITEM_ID_LENGTH + PKT_CLOCK_LENGTH*2 + PKT_DATA_UINT64_LENGTH + PKT_DATA_FLOAT_LENGTH*3)

/* Delete packets */
#define PKT_DELETE_LENGTH \
(PKT_SIZE_LENGTH + PKT_TYPE_LENGTH + PKT_ITEM_ID_LENGTH + PKT_CLOCK_LENGTH)

#define REPLY_DELETE_NUMBER_LENGTH 4
#define REPLY_DELETE_LENGTH \
(PKT_SIZE_LENGTH + PKT_TYPE_LENGTH + REPLY_RESULT_LENGTH + REPLY_DELETE_NUMBER_LENGTH)

/* Packet type */
enum {
	PKT_TYPE_ADD_DATA           = 100,
	PKT_TYPE_GET                = 1000,
	PKT_TYPE_GET_WITH_TIMESTAMP = 1050,
	PKT_TYPE_GET_MIN_CLOCK      = 1100,
	PKT_TYPE_GET_STATISTICS     = 1200,
	PKT_TYPE_DELETE             = 2000,
};

enum {
	SORT_ASCENDING = 0,
	SORT_DESDENDING = 1,
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

#if 0
static int fill_common_header(private_context_t *ctx, uint64_t id,
                              struct timespec *time, uint8_t *buf, uint16_t cmd, int pkt_size)
{
	int idx = 0;

	/* pkt size */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, pkt_size - PKT_SIZE_LENGTH);
	idx += PKT_SIZE_LENGTH;

	/* command */
	*((uint16_t *)&buf[idx]) = conv_le16(ctx, cmd);
	idx += PKT_TYPE_LENGTH;

	/* Item ID */
	*((uint64_t *)&buf[idx]) = conv_le64(ctx, id);
	idx += PKT_ITEM_ID_LENGTH;

	/* clock */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, time->tv_sec);
	idx += PKT_CLOCK_LENGTH;

	/* ns */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, time->tv_nsec);
	idx += PKT_NS_LENGTH;

	return idx;
}
#endif

static int fill_add_data_header(private_context_t *ctx, uint64_t id,
                                struct timespec *time, uint8_t *buf, uint16_t data_type,
                                int pkt_size)
{
	int idx = 0;

	/* pkt size */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, pkt_size - PKT_SIZE_LENGTH);
	idx += PKT_SIZE_LENGTH;

	/* command */
	*((uint16_t *)&buf[idx]) = conv_le16(ctx, PKT_TYPE_ADD_DATA);
	idx += PKT_TYPE_LENGTH;

	/* data type */
	*((uint16_t *)&buf[idx]) = conv_le16(ctx, data_type);
	idx += PKT_DATA_TYPE_LENGTH;

	/* Item ID */
	*((uint64_t *)&buf[idx]) = conv_le64(ctx, id);
	idx += PKT_ITEM_ID_LENGTH;

	/* clock */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, time->tv_sec);
	idx += PKT_CLOCK_LENGTH;

	/* ns */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, time->tv_nsec);
	idx += PKT_NS_LENGTH;

	return idx;
}

static int fill_get_min_clock_packet(private_context_t *ctx, uint8_t *buf, uint64_t id)
{
	int idx = 0;

	/* pkt size */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, PKT_GET_MIN_CLOCK_LENGTH - PKT_SIZE_LENGTH);
	idx += PKT_SIZE_LENGTH;

	/* type */
	*((uint16_t *)&buf[idx]) = conv_le16(ctx, PKT_TYPE_GET_MIN_CLOCK);
	idx += PKT_TYPE_LENGTH;

	/* Item ID */
	*((uint64_t *)&buf[idx]) = conv_le64(ctx, id);
	idx += PKT_ITEM_ID_LENGTH;

	return idx;
}

static int fill_delete_packet(private_context_t *ctx, uint8_t *buf, uint64_t id,
                              struct timespec *threshold)
{
	int idx = 0;

	/* pkt size */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, PKT_DELETE_LENGTH - PKT_SIZE_LENGTH);
	idx += PKT_SIZE_LENGTH;

	/* type */
	*((uint16_t *)&buf[idx]) = conv_le16(ctx, PKT_TYPE_DELETE);
	idx += PKT_TYPE_LENGTH;

	/* Item ID */
	*((uint64_t *)&buf[idx]) = conv_le64(ctx, id);
	idx += PKT_ITEM_ID_LENGTH;

	/* min clock */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, threshold->tv_sec);
	idx += PKT_CLOCK_LENGTH;

	return idx;
}

static int fill_get_statistics(private_context_t *ctx, uint8_t *buf, uint64_t id,
                               struct timespec *time0, struct timespec *time1)
{
	int idx = 0;

	/* pkt size */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, PKT_GET_STATISTICS_LENGTH - PKT_SIZE_LENGTH);
	idx += PKT_SIZE_LENGTH;

	/* type */
	*((uint16_t *)&buf[idx]) = conv_le16(ctx, PKT_TYPE_GET_STATISTICS);
	idx += PKT_TYPE_LENGTH;

	/* Item ID */
	*((uint64_t *)&buf[idx]) = conv_le64(ctx, id);
	idx += PKT_ITEM_ID_LENGTH;

	/* clock0 */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, time0->tv_sec);
	idx += PKT_CLOCK_LENGTH;

	/* clock1 */
	*((uint32_t *)&buf[idx]) = conv_le32(ctx, time1->tv_sec);
	idx += PKT_CLOCK_LENGTH;

	return idx;
}

static int write_data(private_context_t *ctx, uint8_t *buf, size_t count)
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

static int read_data(private_context_t *ctx, uint8_t *buf, size_t count)
{
	uint8_t *ptr = buf;
	while (count > 0) {
		ssize_t read_byte = read(ctx->socket, ptr, count);
		if (read_byte == 0) {
			ERR_MSG("file stream has unexpectedly closed. count: %zd\n",
			        count);
			reset_context(ctx);
			return -1;
		} else if (read_byte == -1) {
			ERR_MSG("Failed to read data: %d\n", errno);
			reset_context(ctx);
			return -1;
		}
		count -= read_byte;
		ptr += read_byte;
	}
	return 0;
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
	idx += PKT_TYPE_LENGTH;
	if (reply_type != expected_pkt_type) {
		ERR_MSG("reply type is not PKT_TYPE_DELETE: %d: %d\n",
		        reply_type, PKT_TYPE_DELETE);
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

static int parse_reply_get_min_clock(private_context_t *ctx, uint8_t *buf,
                                     struct timespec *minimum_time)
{
	uint32_t expected_length = REPLY_GET_MIN_CLOCK_LENGTH - PKT_SIZE_LENGTH;
	int idx = parse_common_reply_header(ctx, buf, NULL, expected_length,
	                                    PKT_TYPE_GET_MIN_CLOCK);
	if (idx == -1)
		return -1;

	// min_clock
	uint32_t _min_clock = restore_le32(ctx, &buf[idx]);
	idx += PKT_CLOCK_LENGTH;
	minimum_time->tv_sec = _min_clock;
	minimum_time->tv_nsec = 0;

	return 0;
}

static int parse_reply_delete(private_context_t *ctx, uint8_t *buf)
{
	uint32_t expected_length = REPLY_DELETE_LENGTH - PKT_SIZE_LENGTH;
	int idx = parse_common_reply_header(ctx, buf, NULL, expected_length,
	                                    PKT_TYPE_DELETE);
	if (idx == -1)
		return -1;

	// Number of the deleted
	uint32_t num_deleted = restore_le32(ctx, &buf[idx]);
	idx += REPLY_DELETE_NUMBER_LENGTH;

	return num_deleted;
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
                            uint64_t id, struct timespec *time, double value)
{
	private_context_t *ctx = get_connected_private_context(_ctx);
	if (ctx == NULL)
		return -1;

	int pkt_size = PKT_ADD_DATA_HEADER_LENGTH + PKT_DATA_FLOAT_LENGTH;
	uint8_t buf[pkt_size];
	uint8_t *ptr = buf;

	/* header */
	ptr += fill_add_data_header(ctx, id, time, ptr,
	                            HISTORY_GLUON_TYPE_FLOAT, pkt_size);

	/* data */
	write_ieee754_double(ctx, ptr, value);

	/* write data */
	if (write_data(ctx, buf, pkt_size) == -1)
		return -1;
	return 0;
}

int history_gluon_add_uint64(history_gluon_context_t _ctx,
                             uint64_t id, struct timespec *time, uint64_t value)
{
	private_context_t *ctx = get_connected_private_context(_ctx);
	if (ctx == NULL)
		return -1;

	int pkt_size = PKT_ADD_DATA_HEADER_LENGTH + PKT_DATA_UINT64_LENGTH;
	uint8_t buf[pkt_size];
	uint8_t *ptr = buf;

	/* header */
	ptr += fill_add_data_header(ctx, id, time, ptr,
	                            HISTORY_GLUON_TYPE_UINT64, pkt_size);

	/* data */
	*((uint64_t *)ptr) = conv_le64(ctx, value);

	/* write data */
	if (write_data(ctx, buf, pkt_size) == -1)
		return -1;
	return 0;
}

int history_gluon_add_string(history_gluon_context_t _ctx,
                             uint64_t id, struct timespec *time, char *value)
{
	private_context_t *ctx = get_connected_private_context(_ctx);
	if (ctx == NULL)
		return -1;

	ERR_MSG("Not implemented yet\n");
	return -1;
}

int history_gluon_add_blob(history_gluon_context_t _ctx,
                           uint64_t id, struct timespec *time, uint8_t *value,
                           uint64_t length)
{
	private_context_t *ctx = get_connected_private_context(_ctx);
	if (ctx == NULL)
		return -1;

	ERR_MSG("Not implemented yet\n");
	return -1;
}

int history_gluon_get_minmum_time(history_gluon_context_t _ctx,
                                  uint64_t id, struct timespec *minimum_time)
{
	private_context_t *ctx = get_connected_private_context(_ctx);
	if (ctx == NULL)
		return -1;

	// request
	uint8_t request[PKT_GET_MIN_CLOCK_LENGTH];
	fill_get_min_clock_packet(ctx, request, id);
	if (write_data(ctx, request, PKT_GET_MIN_CLOCK_LENGTH) == -1)
		return -1;

	// reply
	uint8_t reply[REPLY_GET_MIN_CLOCK_LENGTH];
	if (read_data(ctx, reply, REPLY_GET_MIN_CLOCK_LENGTH) == -1)
		return -1;
	if (parse_reply_get_min_clock(ctx, reply, minimum_time))
		return -1;
	return 0;
}

int history_gluon_range_query(history_gluon_context_t _ctx, uint64_t id,
                              struct timespec *time0, struct timespec *time1,
                              history_gluon_value_array_t *array)
{
	ERR_MSG("Not implemented yet\n");
	return -1;
}

void history_gluon_free_value_array(history_gluon_context_t _ctx,
                                    history_gluon_value_array_t *array)
{
	uint64_t i = 0;
	for (i = 0; i < array->length; i++) {
		history_gluon_value_t *value = &(array->array[i]);
		history_gluon_free_value(_ctx, value);
	}
}

int history_gluon_query(history_gluon_context_t _ctx,
                        uint64_t id, struct timespec *time, int search_near,
                        history_gluon_value_t *value)
{
	ERR_MSG("Not implemented yet\n");
	return -1;
}

void history_gluon_free_value(history_gluon_context_t _ctx,
                              history_gluon_value_t *value)
{
	if (value->type == HISTORY_GLUON_TYPE_STRING)
		free(value->v_string);
	else if (value->type == HISTORY_GLUON_TYPE_BLOB)
		free(value->v_blob);
}

int history_gluon_delete_below_threshold(history_gluon_context_t _ctx,
                                         uint64_t id, struct timespec *threshold,
                                         uint32_t *num_deleted_entries)
{
	private_context_t *ctx = get_connected_private_context(_ctx);
	if (ctx == NULL)
		return -1;

	// request
	uint8_t request[PKT_DELETE_LENGTH];
	fill_delete_packet(ctx, request, id, threshold);
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

int history_gluon_get_statistics(history_gluon_context_t _ctx, uint64_t id,
                                 struct timespec *time0, struct timespec *time1,
                                 history_gluon_statistics_t *statistics)
{
	private_context_t *ctx = get_connected_private_context(_ctx);
	if (ctx == NULL)
		return -1;

	// request
	uint8_t request[PKT_GET_STATISTICS_LENGTH];
	int cmd_length = fill_get_statistics(ctx, request, id, time0, time1);
	if (write_data(ctx, request, cmd_length) == -1)
		return -1;

	// read reply packet
	uint8_t reply[REPLY_STATISTICS_LENGTH];
	if (read_data(ctx, reply, REPLY_STATISTICS_LENGTH) == -1)
		return -1;
	uint32_t expected_length = REPLY_STATISTICS_LENGTH - PKT_SIZE_LENGTH;
	int idx = parse_common_reply_header(ctx, reply, NULL, expected_length,
	                                    PKT_TYPE_GET_STATISTICS);
	if (idx == -1)
		return -1;

	// item ID
	statistics->itemid = restore_le64(ctx, &reply[idx]);
	idx += PKT_ITEM_ID_LENGTH;

	// clock0
	statistics->time0.tv_sec = restore_le32(ctx, &reply[idx]);
	statistics->time0.tv_nsec = 0;
	idx += PKT_CLOCK_LENGTH;

	// clock1
	statistics->time1.tv_sec = restore_le32(ctx, &reply[idx]);
	statistics->time1.tv_nsec = 0;
	idx += PKT_CLOCK_LENGTH;

	// count
	statistics->count = restore_le64(ctx, &reply[idx]);
	idx += PKT_DATA_UINT64_LENGTH;

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

