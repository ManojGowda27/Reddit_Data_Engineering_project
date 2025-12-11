import pytest
from unittest.mock import MagicMock, patch
from etls.reddit_etl import transform_post, RedditClient

# --- TEST 1: LOGIC VALIDATION ---
def test_transform_post_success():
    """
    Verifies that raw data is correctly cleaned and cast to types.
    """
    # 1. Setup: Create 'dirty' input data
    dirty_input = {
        'id': '12345',
        'title': '  Messy Title  ',       # Needs stripping
        'score': '100',                   # String, needs int
        'num_comments': None,             # None, needs default 0
        'over_18': None,                  # None, needs False
        'created_utc': 1710000000,        # Timestamp, needs ISO format
        'author': 'some_user'
    }

    # 2. Execution: Run the function
    result = transform_post(dirty_input)

    # 3. Assertion: Check if logic worked
    assert result['title'] == 'Messy Title'     # Did it strip whitespace?
    assert result['score'] == 100               # Did it cast to int?
    assert isinstance(result['score'], int)
    assert result['num_comments'] == 0          # Did it handle None?
    assert result['over_18'] is False           # Did it default to False?
    assert result['created_utc'] == '2024-03-09T16:00:00+00:00' # Did it convert time?

def test_transform_post_missing_fields():
    """
    Verifies the function doesn't crash if optional fields are missing.
    """
    minimal_input = {'id': '123'} # Missing title, score, etc.
    
    result = transform_post(minimal_input)
    
    # Should use defaults defined in your function
    assert result['num_comments'] == 0
    assert result['author'] == 'Unknown'

# --- TEST 2: MOCKING EXTERNAL API (PRAW) ---
@patch('etls.reddit_etl.praw.Reddit') # Stop code from actually calling Reddit
def test_reddit_client_extraction(mock_reddit_class):
    """
    Tests the RedditClient generator without internet.
    We 'mock' the API response to be a list of fake posts.
    """
    # 1. Setup the Mock
    # Create a fake Reddit instance
    mock_instance = mock_reddit_class.return_value
    # Create a fake subreddit object
    mock_subreddit = mock_instance.subreddit.return_value
    
    # Create a fake Post object (what PRAW usually returns)
    mock_post = MagicMock()
    mock_post.id = "test_id_1"
    mock_post.title = "Test Post"
    mock_post.score = 10
    # Add other attributes your code expects (to avoid AttributeError)
    mock_post.num_comments = 5
    mock_post.author = "tester"
    mock_post.created_utc = 1710000000
    mock_post.url = "http://test.com"
    mock_post.over_18 = False
    mock_post.edited = False
    mock_post.spoiler = False
    mock_post.stickied = False

    # Tell the mock: "When .top() is called, return a list with this fake post"
    mock_subreddit.top.return_value = [mock_post]

    # 2. Execution
    client = RedditClient("fake_id", "fake_secret", "fake_agent")
    # This calls our generator, which calls the mock
    posts_generator = client.extract_posts("dataengineering", "day", 1)
    
    # Convert generator to list to check contents
    posts_list = list(posts_generator)

    # 3. Assertion
    assert len(posts_list) == 1
    assert posts_list[0]['id'] == "test_id_1"
    assert posts_list[0]['title'] == "Test Post"
    
    # Verify that PRAW was actually called with correct parameters
    mock_instance.subreddit.assert_called_with("dataengineering")
    mock_subreddit.top.assert_called_with(time_filter="day", limit=1)