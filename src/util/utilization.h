
#include <unistd.h>
#include <stdio.h>
#include <dirent.h>
#include <sys/stat.h>
#include <iostream>

#include "global.h"

static uint64_t ComputeNodeUtilization()
{
	DIR *d = opendir(g_db_files_dirname.c_str());
	if (!d) 
	{
		throw "error openning db shards directory";
	}

	uint64_t used = 0;
	struct dirent *de;
	for (de = readdir(d); de != NULL; de = readdir(d))
	{
		struct stat s;
		if (lstat(de->d_name, &s) == 0)
		{
			used += s.st_size;
		}
	}
	closedir(d);
	return used;
}