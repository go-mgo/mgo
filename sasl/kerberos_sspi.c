#ifdef _WIN32
#include "kerberos_sspi.h"
#include <stdlib.h>

static HINSTANCE _sspi_security_dll = NULL; 
static HINSTANCE _sspi_secur32_dll = NULL;

/**
 * Encrypt A Message
 */
SECURITY_STATUS SEC_ENTRY _sspi_encrypt_message(PCtxtHandle phContext, unsigned long fQOP, PSecBufferDesc pMessage, unsigned long MessageSeqNo) {
  // Create function pointer instance
  encryptMessage_fn pfn_encryptMessage = NULL;

  // Return error if library not loaded
  if (_sspi_security_dll == NULL) {
    return -1;
  }

  // Map function to library method
  pfn_encryptMessage = (encryptMessage_fn) GetProcAddress(_sspi_security_dll, "EncryptMessage");
  // Check if the we managed to map function pointer
  if (!pfn_encryptMessage) {
    return -2;
  }

  // Call the function
  return (*pfn_encryptMessage)(phContext, fQOP, pMessage, MessageSeqNo);
}

/**
 * Acquire Credentials
 */
SECURITY_STATUS SEC_ENTRY _sspi_acquire_credentials_handle(LPSTR pszPrincipal, LPSTR pszPackage, unsigned long fCredentialUse,
    void* pvLogonId, void* pAuthData, SEC_GET_KEY_FN pGetKeyFn, void* pvGetKeyArgument, PCredHandle phCredential,
    PTimeStamp ptsExpiry) {
  // Create function pointer instance
  acquireCredentialsHandle_fn pfn_acquireCredentialsHandle = NULL;

  // Return error if library not loaded
  if (_sspi_security_dll == NULL) {
    return -1;
  }

  // Map function
  #ifdef _UNICODE
    pfn_acquireCredentialsHandle = (acquireCredentialsHandle_fn) GetProcAddress(_sspi_security_dll, "AcquireCredentialsHandleW");
  #else
    pfn_acquireCredentialsHandle = (acquireCredentialsHandle_fn) GetProcAddress(_sspi_security_dll, "AcquireCredentialsHandleA");
  #endif

  // Check if the we managed to map function pointer
  if (!pfn_acquireCredentialsHandle) {
    return -2;
  }

  // Status
  return (*pfn_acquireCredentialsHandle)(
    pszPrincipal,
    pszPackage,
    fCredentialUse,
    pvLogonId,
    pAuthData,
    pGetKeyFn,
    pvGetKeyArgument,
    phCredential,
    ptsExpiry);
}

/**
 * Initialize Security Context
 */
SECURITY_STATUS SEC_ENTRY _sspi_initialize_security_context(PCredHandle phCredential, PCtxtHandle phContext, LPSTR pszTargetName,
    unsigned long fContextReq, unsigned long Reserved1, unsigned long TargetDataRep, PSecBufferDesc pInput, unsigned long Reserved2,
    PCtxtHandle phNewContext, PSecBufferDesc pOutput, unsigned long * pfContextAttr, PTimeStamp ptsExpiry) {
  // Create function pointer instance
  initializeSecurityContext_fn pfn_initializeSecurityContext = NULL;

  // Return error if library not loaded
  if (_sspi_security_dll == NULL) {
    return -1;
  }
  
  // Map function
  #ifdef _UNICODE
    pfn_initializeSecurityContext = (initializeSecurityContext_fn) GetProcAddress(_sspi_security_dll, "InitializeSecurityContextW");
  #else
    pfn_initializeSecurityContext = (initializeSecurityContext_fn) GetProcAddress(_sspi_security_dll, "InitializeSecurityContextA");
  #endif

  // Check if the we managed to map function pointer
  if (!pfn_initializeSecurityContext) {
    return -2;
  }

  // Execute intialize context
  return (*pfn_initializeSecurityContext)(
    phCredential,
    phContext,
    pszTargetName,
    fContextReq,
    Reserved1,
    TargetDataRep,
    pInput,
    Reserved2,
    phNewContext,
    pOutput,
    pfContextAttr,
    ptsExpiry);
}

/**
 * Query Context Attributes
 */
SECURITY_STATUS SEC_ENTRY _sspi_query_context_attributes(PCtxtHandle phContext, unsigned long ulAttribute, void * pBuffer) {
  // Create function pointer instance
  queryContextAttributes_fn pfn_queryContextAttributes = NULL;

  // Return error if library not loaded
  if (_sspi_security_dll == NULL) {
    return -1;
  }

  #ifdef _UNICODE
    pfn_queryContextAttributes = (queryContextAttributes_fn) GetProcAddress(_sspi_security_dll, "QueryContextAttributesW");
  #else
    pfn_queryContextAttributes = (queryContextAttributes_fn) GetProcAddress(_sspi_security_dll, "QueryContextAttributesA");
  #endif

  // Check if the we managed to map function pointer
  if (!pfn_queryContextAttributes) {
    return -2;
  }

  // Call the function
  return (*pfn_queryContextAttributes)(
    phContext,
    ulAttribute,
    pBuffer);
}

/**
 * Load security.dll dynamically
 */
int _load_library() {
  DWORD err;
  // Load the library
  _sspi_security_dll = LoadLibrary("security.dll");

  // Check if the library loaded
  if (_sspi_security_dll == NULL) {
    err = GetLastError();
    return err;
  }

  // Load the library
  _sspi_secur32_dll = LoadLibrary("secur32.dll");

  // Check if the library loaded
  if (_sspi_secur32_dll == NULL) {
    err = GetLastError();
    return err;
  }

  return 0;
}

#endif
