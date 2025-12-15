# Case Study: Evolving the Reddit ETL Pipeline
*From a basic script to a robust, distributed Data Lakehouse.*

## 1. The Memory Bottleneck (Scalability)

### 🔴 Initial Architecture: Eager Loading
In the first iteration, the extraction logic stored all fetched posts in a standard Python list before processing.

```python
# Initial Approach
all_posts = []
for post in reddit.subreddit('dataengineering').top(limit=10000):
    all_posts.append(post)  # RISK: Stores everything in RAM at once
return all_posts
```
The Limitation: This approach has $O(N)$ memory complexity. If the API returns 1GB of data, the container requires 1GB+ of RAM. On smaller worker nodes, this caused MemoryError and OOM (Out of Memory) terminations during high-volume spikes.
### 🟢 Optimized Architecture: Lazy Streaming
Refactored the extraction layer to use Python Generators (`yield`).

```python
# Optimized Approach
for post in reddit.subreddit('dataengineering').top(limit=None):
    yield transform_post(post)  # BENEFIT: Processes one item at a time
```
**The Result**: Memory usage is now $O(1)$ **(Constant)**. The pipeline can process gigabytes of data with the same minimal RAM footprint as a small dataset, making it capable of handling infinite scale.
#

## 2. The Distributed Race Condition (Reliability)

🔴 **Initial Architecture: Split Tasks**
The Airflow DAG originally separated "Extract" and "Upload" into two distinct tasks relying on local disk storage.

Task A: Saves data to `/tmp/reddit_data.csv`.

Task B: Reads `/tmp/reddit_data.csv` and uploads to S3.

**The Limitation**: In a distributed environment (using Celery or Kubernetes executors), **Task A** might run on Worker Node 1, while **Task B** runs on Worker Node 2. This caused `FileNotFoundError` failures because the workers did not share a filesystem.

🟢 **Optimized Architecture: Atomic Extract-Load**
Implemented an Atomic Extract-Load Pattern.

**The Solution**: Merged extraction and upload into a single atomic operation. Data is streamed from the API directly to S3 within the same execution context.

**The Result**: The pipeline is now Stateless and fully compatible with any distributed executor (Kubernetes, Celery) without the need for complex shared storage solutions like AWS EFS.
#

## 3. Code Maintainability & Testing (Quality Assurance)
🔴 **Initial Architecture: Script-Based**
Logic was written as loose functions within a monolithic script, mixing API connection details directly with transformation logic.

**The Limitation**: Testing was difficult and unreliable. To verify the transformation logic, the test suite had to connect to the live Reddit API. If the API was down or rate-limited, the deployment pipeline would fail.

🟢 **Optimized Architecture: OOP & CI/CD**
Refactored the codebase into Object-Oriented Design (OOP) with Dependency Injection.

**The Solution**: Encapsulated API logic within a `RedditClient` class.

**The Result**: Enabled Unit Testing with Mocks.

**Offline Testing**: We can now test pipeline logic using `unittest.mock` without an internet connection.

**CI/CD Integration**: A GitHub Actions pipeline automatically runs these tests on every `git push`.

**Reliability**: Tests are now deterministic and do not flake due to external API issues.
#

## 4. Data Integrity (Accuracy)
🔴 **Initial Architecture: Naive Timestamps**
Used standard Python `datetime.utcnow()` calls.

**The Limitation**: Python's "naive" datetime objects (lacking timezone info) are ambiguous. Downstream systems (Redshift, BI tools) often misinterpreted these times, leading to data inconsistencies during Daylight Savings Time transitions.

🟢 **Optimized Architecture: Timezone-Aware**
Enforced Timezone-Aware Datetimes throughout the pipeline.

**The Solution**: Updated all logic to use `datetime.fromtimestamp(ts, tz=timezone.utc)`.

**The Result**: Explicit UTC timestamps ensure strict data consistency across global regions and accurate reporting in downstream analytics tools.
#
