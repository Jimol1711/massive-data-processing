from pmaw import PushshiftAPI
import pandas as pd

api = PushshiftAPI()

# Define parameters
subreddit = 'news'
limit = 1500000  # Number of comments to fetch

# Fetch comments
comments = api.search_comments(subreddit=subreddit, limit=limit)

# Convert to DataFrame
comments_df = pd.DataFrame(comments)

# Save to CSV
comments_df.to_csv('r_news_comments.csv', index=False)