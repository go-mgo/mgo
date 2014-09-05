#ifdef _WIN32
#ifndef SSPI_C_H
#define SSPI_C_H

#define SECURITY_WIN32 1

#include <windows.h>
#include <sspi.h>

/**
 * Encrypt A Message
 */
SECURITY_STATUS SEC_ENTRY _sspi_encrypt_message(PCtxtHandle phContext, unsigned long fQOP, PSecBufferDesc pMessage, unsigned long MessageSeqNo);

typedef DWORD (WINAPI *encryptMessage_fn)(PCtxtHandle phContext, ULONG fQOP, PSecBufferDesc pMessage, ULONG MessageSeqNo);  

/**
 * Acquire Credentials
 */
SECURITY_STATUS SEC_ENTRY _sspi_acquire_credentials_handle(
  LPSTR pszPrincipal,                 // Name of principal
  LPSTR pszPackage,                   // Name of package
  unsigned long fCredentialUse,       // Flags indicating use
  void * pvLogonId,                   // Pointer to logon ID
  void * pAuthData,                   // Package specific data
  SEC_GET_KEY_FN pGetKeyFn,           // Pointer to GetKey() func
  void * pvGetKeyArgument,            // Value to pass to GetKey()
  PCredHandle phCredential,           // (out) Cred Handle
  PTimeStamp ptsExpiry                // (out) Lifetime (optional)
);

typedef DWORD (WINAPI *acquireCredentialsHandle_fn)(
    LPSTR pszPrincipal, LPSTR pszPackage, unsigned long fCredentialUse,
    void * pvLogonId, void * pAuthData, SEC_GET_KEY_FN pGetKeyFn, void * pvGetKeyArgument,
    PCredHandle phCredential, PTimeStamp ptsExpiry
  );

/**
 * Initialize Security Context
 */
SECURITY_STATUS SEC_ENTRY _sspi_initialize_security_context(
  PCredHandle phCredential,       // Cred to base context
  PCtxtHandle phContext,          // Existing context (OPT)
  LPSTR pszTargetName,            // Name of target
  unsigned long fContextReq,      // Context Requirements
  unsigned long Reserved1,        // Reserved, MBZ
  unsigned long TargetDataRep,    // Data rep of target
  PSecBufferDesc pInput,          // Input Buffers
  unsigned long Reserved2,        // Reserved, MBZ
  PCtxtHandle phNewContext,       // (out) New Context handle
  PSecBufferDesc pOutput,         // (inout) Output Buffers
  unsigned long * pfContextAttr,  // (out) Context attrs
  PTimeStamp ptsExpiry            // (out) Life span (OPT)
);

typedef DWORD (WINAPI *initializeSecurityContext_fn)(
  PCredHandle phCredential, PCtxtHandle phContext, LPSTR pszTargetName, unsigned long fContextReq, 
  unsigned long Reserved1, unsigned long TargetDataRep, PSecBufferDesc pInput, unsigned long Reserved2,
  PCtxtHandle phNewContext, PSecBufferDesc pOutput, unsigned long * pfContextAttr, PTimeStamp ptsExpiry);

/**
 * Query Context Attributes
 */
SECURITY_STATUS SEC_ENTRY _sspi_query_context_attributes(
  PCtxtHandle phContext,          // Context to query
  unsigned long ulAttribute,      // Attribute to query
  void * pBuffer                  // Buffer for attributes
);

typedef DWORD (WINAPI *queryContextAttributes_fn)(
  PCtxtHandle phContext, unsigned long ulAttribute, void * pBuffer);

/**
 * Load security.dll dynamically
 */
int _load_library();

#endif
#endif
