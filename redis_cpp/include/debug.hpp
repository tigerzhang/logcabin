#ifndef DEBUG_HPP
#define DEBUG_HPP

/*
Macro for debugging
*/

#ifdef DEBUG
#define debug_print(fmt, ...) fprintf(stderr, fmt, __VA_ARGS__)
#else
#define debug_print(fmt, ...) do {} while (0)
#endif

#endif
