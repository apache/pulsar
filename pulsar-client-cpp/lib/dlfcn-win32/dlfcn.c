/*
 * dlfcn-win32
 * Copyright (c) 2007 Ramiro Polla
 * Copyright (c) 2015 Tiancheng "Timothy" Gu
 * Copyright (c) 2019 Pali Rohár <pali.rohar@gmail.com>
 *
 * dlfcn-win32 is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * dlfcn-win32 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with dlfcn-win32; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 */

#ifdef _DEBUG
#define _CRTDBG_MAP_ALLOC
#include <stdlib.h>
#include <crtdbg.h>
#endif
#define PSAPI_VERSION 1
#include <windows.h>
#include <psapi.h>
#include <stdio.h>
#include <stdlib.h>

#ifdef _MSC_VER
/* https://docs.microsoft.com/en-us/cpp/intrinsics/returnaddress */
#include <intrin.h>
#pragma intrinsic(_ReturnAddress)
#else
/* https://gcc.gnu.org/onlinedocs/gcc/Return-Address.html */
#ifndef _ReturnAddress
#define _ReturnAddress() (__builtin_extract_return_addr(__builtin_return_address(0)))
#endif
#endif

#ifdef SHARED
#define DLFCN_WIN32_EXPORTS
#endif
#include "dlfcn.h"

#if ((defined(_WIN32) || defined(WIN32)) && (defined(_MSC_VER)) )
#define snprintf sprintf_s
#endif

#ifdef UNICODE
#include <wchar.h>
#define CHAR	wchar_t
#define UNICODE_L(s)	L##s
#else
#define CHAR	char
#define UNICODE_L(s)	s
#endif

/* Note:
 * MSDN says these functions are not thread-safe. We make no efforts to have
 * any kind of thread safety.
 */

typedef struct local_object {
    HMODULE hModule;
    struct local_object *previous;
    struct local_object *next;
} local_object;

static local_object first_object;

/* These functions implement a double linked list for the local objects. */
static local_object *local_search( HMODULE hModule )
{
    local_object *pobject;

    if( hModule == NULL )
        return NULL;

    for( pobject = &first_object; pobject; pobject = pobject->next )
        if( pobject->hModule == hModule )
            return pobject;

    return NULL;
}

static void local_add( HMODULE hModule )
{
    local_object *pobject;
    local_object *nobject;

    if( hModule == NULL )
        return;

    pobject = local_search( hModule );

    /* Do not add object again if it's already on the list */
    if( pobject )
        return;

    for( pobject = &first_object; pobject->next; pobject = pobject->next );

    nobject = (local_object*) malloc( sizeof( local_object ) );

    /* Should this be enough to fail local_add, and therefore also fail
     * dlopen?
     */
    if( !nobject )
        return;

    pobject->next = nobject;
    nobject->next = NULL;
    nobject->previous = pobject;
    nobject->hModule = hModule;
}

static void local_rem( HMODULE hModule )
{
    local_object *pobject;

    if( hModule == NULL )
        return;

    pobject = local_search( hModule );

    if( !pobject )
        return;

    if( pobject->next )
        pobject->next->previous = pobject->previous;
    if( pobject->previous )
        pobject->previous->next = pobject->next;

    free( pobject );
}

/* POSIX says dlerror( ) doesn't have to be thread-safe, so we use one
 * static buffer.
 * MSDN says the buffer cannot be larger than 64K bytes, so we set it to
 * the limit.
 */
static CHAR error_buffer[65535];
static CHAR *current_error;
static char dlerror_buffer[65536];

static int copy_string( CHAR *dest, int dest_size, const CHAR *src )
{
    int i = 0;

    /* gcc should optimize this out */
    if( !src || !dest )
        return 0;

    for( i = 0 ; i < dest_size-1 ; i++ )
    {
        if( !src[i] )
            break;
        else
            dest[i] = src[i];
    }
    dest[i] = '\0';

    return i;
}

static void save_err_str( const CHAR *str )
{
    DWORD dwMessageId;
    DWORD pos;

    dwMessageId = GetLastError( );

    if( dwMessageId == 0 )
        return;

    /* Format error message to:
     * "<argument to function that failed>": <Windows localized error message>
      */
    pos  = copy_string( error_buffer,     sizeof(error_buffer),     UNICODE_L("\"") );
    pos += copy_string( error_buffer+pos, sizeof(error_buffer)-pos, str );
    pos += copy_string( error_buffer+pos, sizeof(error_buffer)-pos, UNICODE_L("\": ") );
    pos += FormatMessage( FORMAT_MESSAGE_FROM_SYSTEM, NULL, dwMessageId,
        MAKELANGID( LANG_NEUTRAL, SUBLANG_DEFAULT ),
        error_buffer+pos, sizeof(error_buffer)-pos, NULL );

    if( pos > 1 )
    {
        /* POSIX says the string must not have trailing <newline> */
        if( error_buffer[pos-2] == '\r' && error_buffer[pos-1] == '\n' )
            error_buffer[pos-2] = '\0';
    }

    current_error = error_buffer;
}

static void save_err_ptr_str( const void *ptr )
{
    CHAR ptr_buf[19]; /* 0x<pointer> up to 64 bits. */

#ifdef UNICODE

#	if ((defined(_WIN32) || defined(WIN32)) && (defined(_MSC_VER)) )
    swprintf_s( ptr_buf, 19, UNICODE_L("0x%p"), ptr );
#	else
    swprintf(ptr_buf, 19, UNICODE_L("0x%p"), ptr);
#	endif

#else
    snprintf( ptr_buf, 19, "0x%p", ptr );
#endif

    save_err_str( ptr_buf );
}

void *dlopen( const char *file, int mode )
{
    HMODULE hModule;
    UINT uMode;

    current_error = NULL;

    /* Do not let Windows display the critical-error-handler message box */
    uMode = SetErrorMode( SEM_FAILCRITICALERRORS );

    if( file == 0 )
    {
        /* POSIX says that if the value of file is 0, a handle on a global
         * symbol object must be provided. That object must be able to access
         * all symbols from the original program file, and any objects loaded
         * with the RTLD_GLOBAL flag.
         * The return value from GetModuleHandle( ) allows us to retrieve
         * symbols only from the original program file. For objects loaded with
         * the RTLD_GLOBAL flag, we create our own list later on. For objects
         * outside of the program file but already loaded (e.g. linked DLLs)
         * they are added below.
         */
        hModule = GetModuleHandle( NULL );

        if( !hModule )
            save_err_ptr_str( file );
    }
    else
    {
        HANDLE hCurrentProc;
        DWORD dwProcModsBefore, dwProcModsAfter;
        CHAR lpFileName[MAX_PATH];
        size_t i;

        /* MSDN says backslashes *must* be used instead of forward slashes. */
        for( i = 0 ; i < sizeof(lpFileName) - 1 ; i ++ )
        {
            if( !file[i] )
                break;
            else if( file[i] == '/' )
                lpFileName[i] = '\\';
            else
                lpFileName[i] = file[i];
        }
        lpFileName[i] = '\0';

        hCurrentProc = GetCurrentProcess( );

        if( EnumProcessModules( hCurrentProc, NULL, 0, &dwProcModsBefore ) == 0 )
            dwProcModsBefore = 0;

        /* POSIX says the search path is implementation-defined.
         * LOAD_WITH_ALTERED_SEARCH_PATH is used to make it behave more closely
         * to UNIX's search paths (start with system folders instead of current
         * folder).
         */
        hModule = LoadLibraryEx(lpFileName, NULL, 
                                LOAD_WITH_ALTERED_SEARCH_PATH );

        if( EnumProcessModules( hCurrentProc, NULL, 0, &dwProcModsAfter ) == 0 )
            dwProcModsAfter = 0;

        /* If the object was loaded with RTLD_LOCAL, add it to list of local
         * objects, so that its symbols cannot be retrieved even if the handle for
         * the original program file is passed. POSIX says that if the same
         * file is specified in multiple invocations, and any of them are
         * RTLD_GLOBAL, even if any further invocations use RTLD_LOCAL, the
         * symbols will remain global. If number of loaded modules was not
         * changed after calling LoadLibraryEx(), it means that library was
         * already loaded.
         */
        if( !hModule )
            save_err_str( lpFileName );
        else if( (mode & RTLD_LOCAL) && dwProcModsBefore != dwProcModsAfter )
            local_add( hModule );
        else if( !(mode & RTLD_LOCAL) && dwProcModsBefore == dwProcModsAfter )
            local_rem( hModule );
    }

    /* Return to previous state of the error-mode bit flags. */
    SetErrorMode( uMode );

    return (void *) hModule;
}

int dlclose( void *handle )
{
    HMODULE hModule = (HMODULE) handle;
    BOOL ret;

    current_error = NULL;

    ret = FreeLibrary( hModule );

    /* If the object was loaded with RTLD_LOCAL, remove it from list of local
     * objects.
     */
    if( ret )
        local_rem( hModule );
    else
        save_err_ptr_str( handle );

    /* dlclose's return value in inverted in relation to FreeLibrary's. */
    ret = !ret;

    return (int) ret;
}

__declspec(noinline) /* Needed for _ReturnAddress() */
void *dlsym( void *handle, const char *name )
{
    FARPROC symbol;
    HMODULE hCaller;
    HMODULE hModule;
    HANDLE hCurrentProc;

#ifdef UNICODE
    wchar_t namew[MAX_PATH];
    wmemset(namew, 0, MAX_PATH);
#endif

    current_error = NULL;
    symbol = NULL;
    hCaller = NULL;
    hModule = GetModuleHandle( NULL );
    hCurrentProc = GetCurrentProcess( );

    if( handle == RTLD_DEFAULT )
    {
        /* The symbol lookup happens in the normal global scope; that is,
         * a search for a symbol using this handle would find the same
         * definition as a direct use of this symbol in the program code.
         * So use same lookup procedure as when filename is NULL.
         */
        handle = hModule;
    }
    else if( handle == RTLD_NEXT )
    {
        /* Specifies the next object after this one that defines name.
         * This one refers to the object containing the invocation of dlsym().
         * The next object is the one found upon the application of a load
         * order symbol resolution algorithm. To get caller function of dlsym()
         * use _ReturnAddress() intrinsic. To get HMODULE of caller function
         * use undocumented hack from https://stackoverflow.com/a/2396380
         * The HMODULE of a DLL is the same value as the module's base address.
         */
        MEMORY_BASIC_INFORMATION info;
        size_t sLen;
        sLen = VirtualQueryEx( hCurrentProc, _ReturnAddress(), &info, sizeof( info ) );
        if( sLen != sizeof( info ) )
            goto end;
        hCaller = (HMODULE) info.AllocationBase;
        if(!hCaller)
            goto end;
    }

    if( handle != RTLD_NEXT )
    {
        symbol = GetProcAddress( (HMODULE) handle, name );

        if( symbol != NULL )
            goto end;
    }

    /* If the handle for the original program file is passed, also search
     * in all globally loaded objects.
     */

    if( hModule == handle || handle == RTLD_NEXT )
    {
        HMODULE *modules;
        DWORD cbNeeded;
        DWORD dwSize;
        size_t i;

        /* GetModuleHandle( NULL ) only returns the current program file. So
         * if we want to get ALL loaded module including those in linked DLLs,
         * we have to use EnumProcessModules( ).
         */
        if( EnumProcessModules( hCurrentProc, NULL, 0, &dwSize ) != 0 )
        {
            modules = malloc( dwSize );
            if( modules )
            {
                if( EnumProcessModules( hCurrentProc, modules, dwSize, &cbNeeded ) != 0 && dwSize == cbNeeded )
                {
                    for( i = 0; i < dwSize / sizeof( HMODULE ); i++ )
                    {
                        if( handle == RTLD_NEXT && hCaller )
                        {
                            /* Next modules can be used for RTLD_NEXT */
                            if( hCaller == modules[i] )
                                hCaller = NULL;
                            continue;
                        }
                        if( local_search( modules[i] ) )
                            continue;
                        symbol = GetProcAddress( modules[i], name );
                        if( symbol != NULL )
                            goto end;
                    }

                }
                free( modules );
            }
        }
    }

end:
    if( symbol == NULL )
    {
#ifdef UNICODE
        size_t converted_chars;

        size_t str_len = strlen(name) + 1;

#if ((defined(_WIN32) || defined(WIN32)) && (defined(_MSC_VER)) )
        errno_t err = mbstowcs_s(&converted_chars, namew, str_len, name, str_len);
        if (err != 0)
            return NULL;
#else
        mbstowcs(namew, name, str_len);
#endif

        save_err_str( namew );
#else
        save_err_str( name );
#endif
    }

    //  warning C4054: 'type cast' : from function pointer 'FARPROC' to data pointer 'void *'
#ifdef _MSC_VER
#pragma warning( suppress: 4054 )
#endif
    return (void*) symbol;
}

char *dlerror( void )
{
    char *error_pointer = dlerror_buffer;
    
    /* If this is the second consecutive call to dlerror, return NULL */
    if (current_error == NULL)
    {
        return NULL;
    }

#ifdef UNICODE
    errno_t err = 0;
    size_t converted_chars = 0;
    size_t str_len = wcslen(current_error) + 1;
    memset(error_pointer, 0, 65535);

#	if ((defined(_WIN32) || defined(WIN32)) && (defined(_MSC_VER)) )
    err = wcstombs_s(&converted_chars, 
        error_pointer, str_len * sizeof(char),
        current_error, str_len * sizeof(wchar_t));

    if (err != 0)
        return NULL;
#	else
    wcstombs(error_pointer, current_error, str_len);
#	endif

#else
    memcpy(error_pointer, current_error, strlen(current_error) + 1);
#endif

    /* POSIX says that invoking dlerror( ) a second time, immediately following
     * a prior invocation, shall result in NULL being returned.
     */
    current_error = NULL;

    return error_pointer;
}

#ifdef SHARED
BOOL WINAPI DllMain( HINSTANCE hinstDLL, DWORD fdwReason, LPVOID lpvReserved )
{
    (void) hinstDLL;
    (void) fdwReason;
    (void) lpvReserved;
    return TRUE;
}
#endif
