import pytest
import os
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, AsyncMock
import httpx

from extract.api_client import QQCatalystClient
from extract.extractor import QQCatalystExtractor
from models.raw_models import RawContact

@pytest.fixture
def api_client():
    """Create API client with test credentials"""
    return QQCatalystClient(
        client_id="test_client_id",
        client_secret="test_client_secret",
        username="test_user",
        password="test_pass"
    )

@pytest.fixture
def mock_httpx_client():
    """Mock httpx client responses"""
    with patch('httpx.Client') as mock_client:
        # Mock successful token response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'access_token': 'test_token',
            'expires_in': 3600
        }
        mock_client.return_value.post.return_value = mock_response
        
        # Mock successful API response
        mock_api_response = Mock()
        mock_api_response.status_code = 200
        mock_api_response.json.return_value = {
            'items': [
                {
                    'id': '123',
                    'name': 'Test Contact',
                    'email': 'test@example.com'
                }
            ]
        }
        mock_client.return_value.get.return_value = mock_api_response
        
        yield mock_client

@pytest.fixture
def mock_db_session():
    """Create a mock database session"""
    session = Mock()
    session.bulk_save_objects = Mock()
    session.commit = Mock()
    session.rollback = Mock()
    return session

@pytest.mark.asyncio
async def test_api_client_auth(api_client, mock_httpx_client):
    """Test API client authentication"""
    # Verify token is obtained during initialization
    assert api_client._access_token == 'test_token'
    assert api_client._token_expires_at is not None
    
    # Verify headers are set correctly
    headers = api_client._get_headers()
    assert headers['Authorization'] == 'Bearer test_token'
    assert headers['Content-Type'] == 'application/json'

@pytest.mark.asyncio
async def test_get_resource(api_client, mock_httpx_client):
    """Test getting a resource from the API"""
    response = await api_client.get_resource('contacts')
    assert response['items'][0]['id'] == '123'
    assert response['items'][0]['name'] == 'Test Contact'

@pytest.mark.asyncio
async def test_get_paginated_resource(api_client, mock_httpx_client):
    """Test paginated resource retrieval"""
    pages = []
    async for page in api_client.get_paginated_resource('contacts'):
        pages.append(page)
    
    assert len(pages) == 1
    assert pages[0]['items'][0]['id'] == '123'

@pytest.mark.asyncio
async def test_token_refresh(api_client, mock_httpx_client):
    """Test token refresh when expired"""
    # Set token to be expired
    api_client._token_expires_at = datetime.now() - timedelta(minutes=5)
    
    # Make a request - should trigger token refresh
    await api_client.get_resource('contacts')
    
    # Verify token refresh was called
    mock_httpx_client.return_value.post.assert_called_with(
        api_client.token_url,
        data={
            'grant_type': 'password',
            'client_id': 'test_client_id',
            'client_secret': 'test_client_secret',
            'username': 'test_user',
            'password': 'test_pass',
            'scope': 'read write'
        }
    )

@pytest.mark.asyncio
async def test_rate_limit_retry(api_client, mock_httpx_client):
    """Test retry behavior on rate limit"""
    # Mock rate limit response followed by success
    rate_limit_response = Mock()
    rate_limit_response.status_code = 429
    success_response = Mock()
    success_response.status_code = 200
    success_response.json.return_value = {'items': []}
    
    mock_httpx_client.return_value.get.side_effect = [
        httpx.HTTPStatusError('Rate limit', request=Mock(), response=rate_limit_response),
        success_response
    ]
    
    # Should succeed after retry
    response = await api_client.get_resource('contacts')
    assert response == {'items': []}

# Extractor Tests

@pytest.mark.asyncio
async def test_extract_resource(api_client, mock_db_session):
    """Test extracting a single resource"""
    extractor = QQCatalystExtractor(api_client, mock_db_session)
    
    # Extract contacts
    count = await extractor.extract_resource('contacts')
    
    assert count == 1
    mock_db_session.bulk_save_objects.assert_called_once()
    mock_db_session.commit.assert_called_once()

@pytest.mark.asyncio
async def test_extract_resource_with_batch(api_client, mock_db_session):
    """Test extracting resources with batching"""
    # Mock API to return multiple pages
    mock_responses = [
        {'items': [{'id': str(i), 'name': f'Contact {i}'} for i in range(1500)]}
    ]
    
    with patch.object(api_client, 'get_paginated_resource') as mock_get:
        mock_get.return_value.__aiter__.return_value = mock_responses
        
        extractor = QQCatalystExtractor(api_client, mock_db_session, batch_size=1000)
        count = await extractor.extract_resource('contacts')
        
        assert count == 1500
        # Should have called bulk_save_objects twice (1000 records + 500 records)
        assert mock_db_session.bulk_save_objects.call_count == 2
        assert mock_db_session.commit.call_count == 2

@pytest.mark.asyncio
async def test_extract_resource_error_handling(api_client, mock_db_session):
    """Test error handling during extraction"""
    extractor = QQCatalystExtractor(api_client, mock_db_session)
    
    # Test invalid resource type
    with pytest.raises(ValueError, match="Unsupported resource type"):
        await extractor.extract_resource('invalid_resource')
    
    # Test database error
    mock_db_session.bulk_save_objects.side_effect = Exception("Database error")
    with pytest.raises(Exception, match="Database error"):
        await extractor.extract_resource('contacts')
    mock_db_session.rollback.assert_called_once()

@pytest.mark.asyncio
async def test_extract_all_resources(api_client, mock_db_session):
    """Test extracting all resources"""
    extractor = QQCatalystExtractor(api_client, mock_db_session)
    
    # Mock successful extraction for each resource
    with patch.object(extractor, 'extract_resource', return_value=1) as mock_extract:
        results = await extractor.extract_all_resources()
        
        # Verify base resources were extracted
        assert all(count == 1 for count in results.values())
        assert 'contacts' in results
        assert 'policies' in results
        assert mock_extract.call_count > 0 