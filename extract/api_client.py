import httpx
from tenacity import retry, stop_after_attempt, wait_exponential
import logging
from typing import Dict, Any, Optional
import os
from datetime import datetime, timedelta
import time
import asyncio
import aiohttp
from aiohttp import ClientError
from aiohttp.client_exceptions import ClientResponseError as HTTPStatusError

logger = logging.getLogger(__name__)

class QQCatalystClient:
    """Client for interacting with QQCatalyst API with OAuth 2.0 authentication"""
    
    def __init__(
        self,
        client_id: str = None,
        client_secret: str = None,
        username: str = None,
        password: str = None,
        base_url: str = None,
        token_url: str = None
    ):
        self.base_url = base_url or os.getenv('QQCATALYST_BASE_URL', "https://api.qqcatalyst.com/v1")
        self.token_url = token_url or os.getenv('QQCATALYST_TOKEN_URL', "https://login.qqcatalyst.com/oauth/token")
        
        # OAuth credentials
        self.client_id = client_id or os.getenv('QQCATALYST_CLIENT_ID')
        self.client_secret = client_secret or os.getenv('QQCATALYST_CLIENT_SECRET')
        self.username = username or os.getenv('QQCATALYST_USERNAME')
        self.password = password or os.getenv('QQCATALYST_PASSWORD')
        
        if not all([self.client_id, self.client_secret, self.username, self.password]):
            raise ValueError("All OAuth credentials are required")
        
        self.client = httpx.Client(timeout=30.0)
        self._access_token = None
        self._token_expires_at = None
        
        # Get initial token
        self._refresh_token()
        
    def _get_new_token(self) -> Dict[str, Any]:
        """Get a new OAuth token using password credentials flow"""
        try:
            response = self.client.post(
                self.token_url,
                data={
                    'grant_type': 'password',
                    'client_id': self.client_id,
                    'client_secret': self.client_secret,
                    'username': self.username,
                    'password': self.password,
                    'scope': 'read write'
                }
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to obtain OAuth token: {str(e)}")
            raise
    
    def _refresh_token(self) -> None:
        """Refresh the OAuth token if expired or about to expire"""
        if (
            not self._access_token or
            not self._token_expires_at or
            datetime.now() >= self._token_expires_at - timedelta(minutes=5)
        ):
            token_data = self._get_new_token()
            self._access_token = token_data['access_token']
            # Set expiration time (usually 1 hour from now, subtract 5 minutes for safety)
            expires_in = token_data.get('expires_in', 3600)
            self._token_expires_at = datetime.now() + timedelta(seconds=expires_in)
            logger.info("OAuth token refreshed successfully")
    
    def _get_headers(self) -> Dict[str, str]:
        """Get headers for API requests including OAuth token"""
        self._refresh_token()  # Ensure token is valid
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True
    )
    async def get_resource(
        self,
        resource: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Get data from a QQCatalyst resource with retry logic and token refresh
        
        Args:
            resource: API resource path (e.g., "contacts", "policies")
            params: Query parameters for pagination and filtering
            
        Returns:
            API response data
        """
        url = f"{self.base_url}/{resource}"
        logger.debug(f"Making request to: {url}")
        logger.debug(f"With parameters: {params}")
        
        try:
            response = self.client.get(
                url,
                headers=self._get_headers(),
                params=params or {}
            )
            
            if response.status_code == 401:
                # Token might be expired, refresh and retry once
                logger.info("Received 401, refreshing token and retrying...")
                self._refresh_token()
                response = self.client.get(
                    url,
                    headers=self._get_headers(),
                    params=params or {}
                )
            
            response.raise_for_status()
            data = response.json()
            logger.debug(f"Response status: {response.status_code}")
            logger.debug(f"Response data: {data}")
            return data
            
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error response body: {e.response.text}")
            if e.response.status_code == 429:  # Rate limit
                logger.warning(f"Rate limit hit for {resource}. Retrying...")
                raise
            elif e.response.status_code >= 500:  # Server error
                logger.error(f"Server error for {resource}: {e}")
                raise
            else:
                logger.error(f"HTTP error for {resource}: {e}")
                raise
                
        except Exception as e:
            logger.error(f"Unexpected error fetching {resource}: {e}")
            raise
            
    async def get_paginated_resource(
        self,
        resource: str,
        last_modified_start: Optional[str] = None,
        last_modified_end: Optional[str] = None,
        page_size: int = 100
    ) -> Dict[str, Any]:
        """
        Get paginated data from a QQCatalyst resource
        
        Args:
            resource: API resource path
            last_modified_start: ISO timestamp for incremental loads (start date)
            last_modified_end: ISO timestamp for incremental loads (end date)
            page_size: Number of records per page
            
        Yields:
            Each page of API response data
        """
        page = 1
        total_items = None
        
        while True:
            params = {
                "pageNumber": page,
                "pageSize": page_size
            }
            
            if "LastModifiedCreated" in resource:
                # Handle LastModifiedCreated endpoint format
                if last_modified_start:
                    params["startDate"] = last_modified_start
                    # Set endDate to current time if not provided
                    params["endDate"] = last_modified_end or datetime.now().isoformat()
            else:
                # Handle standard pagination format
                if last_modified_start:
                    params["lastModifiedStart"] = last_modified_start
                if last_modified_end:
                    params["lastModifiedEnd"] = last_modified_end
                
            data = await self.get_resource(resource, params)
            
            # Handle both dict and list responses for has_data
            has_data = False
            if isinstance(data, dict):
                if data.get("Data") and len(data.get("Data", [])) > 0:
                    has_data = True
                elif data.get("items") and len(data.get("items", [])) > 0:
                    has_data = True
            elif isinstance(data, list):
                if len(data) > 0:
                    has_data = True
            
            if not has_data or (isinstance(data, dict) and total_items is not None and data.get("TotalItems", 0) == 0):
                break
                
            # Get total items count if available
            if isinstance(data, dict) and total_items is None and "TotalItems" in data:
                total_items = data.get("TotalItems")
                logger.info(f"Total items for {resource}: {total_items}")
                
            yield data
            
            # Check if we've reached the last page
            if isinstance(data, dict) and data.get("PageNumber") and data.get("PagesTotal"):
                if data.get("PageNumber") >= data.get("PagesTotal"):
                    break
                    
            page += 1 
            
    async def close(self):
        """Close the HTTP client"""
        if hasattr(self, 'client') and self.client:
            self.client.close() 

    def get(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Make a GET request to the API
        
        Args:
            endpoint: API endpoint to call
            params: Optional query parameters
            
        Returns:
            Response data or None if request failed
        """
        try:
            # Ensure we have a valid token
            if not self._access_token or self._is_token_expired():
                self._refresh_token()
                
            # Make request
            headers = {
                "Authorization": f"Bearer {self._access_token}",
                "Accept": "application/json"
            }
            
            response = self.client.get(
                f"{self.base_url}/{endpoint}",
                headers=headers,
                params=params
            )
            
            # Handle different response formats
            if response.status_code == 200:
                try:
                    data = response.json()
                    
                    # Handle different response structures
                    if isinstance(data, list):
                        # If response is a list, wrap it in a Data field
                        return {"Data": data}
                    elif isinstance(data, dict):
                        # If response has a Data field, return as is
                        if "Data" in data:
                            return data
                        # If response is a single object, wrap it in a Data field
                        else:
                            return {"Data": [data]}
                    else:
                        logger.error(f"Unexpected response format: {type(data)}")
                        return None
                        
                except ValueError as e:
                    logger.error(f"Error parsing JSON response: {str(e)}")
                    return None
                    
            elif response.status_code == 404:
                logger.warning(f"Endpoint not found: {endpoint}")
                return None
                
            elif response.status_code == 401:
                logger.error("Authentication failed - token may be invalid")
                self._access_token = None
                return None
                
            else:
                logger.error(f"API request failed with status {response.status_code}: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error making API request: {str(e)}")
            return None 

    async def _make_request(self, method: str, endpoint: str, params: Optional[Dict] = None, data: Optional[Dict] = None) -> Dict:
        """Make an API request with retry logic and error handling"""
        url = f"{self.base_url}/{endpoint}"
        headers = await self._get_headers()
        
        for attempt in range(self.max_retries):
            try:
                async with self.session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=params,
                    json=data
                ) as response:
                    if response.status == 200:
                        # Handle both list and dictionary responses
                        response_data = await response.json()
                        if isinstance(response_data, list):
                            return {"data": response_data}
                        return response_data
                    elif response.status == 401:
                        # Token expired, refresh and retry
                        await self._refresh_token()
                        continue
                    elif response.status == 404:
                        raise HTTPStatusError(f"Resource not found: {url}")
                    elif response.status == 405:
                        # Try POST if GET fails
                        if method == "GET":
                            return await self._make_request("POST", endpoint, params, data)
                        raise HTTPStatusError(f"Method not allowed: {method} {url}")
                    else:
                        response.raise_for_status()
                        
            except aiohttp.ClientError as e:
                if attempt == self.max_retries - 1:
                    raise
                await asyncio.sleep(self.retry_delay * (attempt + 1))
                
        raise HTTPStatusError(f"Failed after {self.max_retries} attempts: {url}") 