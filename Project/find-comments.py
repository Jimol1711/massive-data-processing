import zstandard as zstd
import json

subreddit = "news"
output_file = "r_news_filtered.json"
count = 0
limit = 1000000

with open(output_file, "w", encoding="utf-8") as out_f:
    with open("RC_2022-06.zst", "rb") as fh:  # Replace with desired month
        dctx = zstd.ZstdDecompressor()
        stream_reader = dctx.stream_reader(fh)
        for line in stream_reader:
            try:
                comment = json.loads(line)
                if comment.get("subreddit") == subreddit:
                    out_f.write(json.dumps(comment) + "\n")
                    count += 1
                    if count >= limit:
                        break
            except Exception:
                continue
