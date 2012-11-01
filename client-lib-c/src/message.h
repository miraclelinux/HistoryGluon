#ifndef _message_h

void history_gluon_message(char *fmt, ...);
void history_gluon_error(char *fmt, ...);

#define INFO(FMT, ...) \
(history_gluon_message("%s: %d: " FMT, __FILE__, __LINE__, ##__VA_ARGS__))

#define ERR_MSG(FMT, ...) \
(history_gluon_error("%s: %d: " FMT, __FILE__, __LINE__, ##__VA_ARGS__))

#endif //_message_h
