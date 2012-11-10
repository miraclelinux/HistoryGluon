#include <cstdio>
#include <cstdlib>
#include <map>
#include <string>
#include <vector>

using namespace std;

typedef bool (*command_handler_fn)(const vector<string> &args);
typedef map<string, command_handler_fn> command_map_t;
typedef command_map_t::iterator command_map_itr;

static command_map_t g_command_map;

bool command_handler_query_all(void)
{
}

bool command_handler_query(const vector<string> &args)
{
	if (args.size() < 1) {
		printf("Error: query command needs args.\n");
		return false;
	}
	if (args[0] == "all")
		return command_handler_query_all();
	return true;
}

static void print_usage(void)
{
	printf("Usage:\n");
	printf("\n");
	printf(" $ history-gluon-cli command args\n");
	printf("\n");
	printf("*** command list ***\n");
	printf("  query all\n");
	//printf("  query id sec ns\n");
	printf("\n");
}

static void init(void)
{
	g_command_map["query"] = command_handler_query;
}

int main(int argc, char **argv)
{
	init();

	if (argc < 2) {
		print_usage();
		return EXIT_FAILURE;;
	}

	char *command = argv[1];
	vector<string> args;
	for (int i = 2; i < argc; i++)
		args.push_back(argv[i]);
	command_map_itr it = g_command_map.find(command);
	if (it == g_command_map.end()) {
		printf("Error: unknown command: %s\n", command);
		return EXIT_FAILURE;
	}

	command_handler_fn command_handler = it->second;
	bool result = (*command_handler)(args);
	if (!result) {
		return EXIT_FAILURE;
	}

	return EXIT_SUCCESS;
}
