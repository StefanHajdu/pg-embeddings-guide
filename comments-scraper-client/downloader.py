import asyncio
import aiohttp
import json
import time
import multiprocessing as mp

with open("../data/comments/drinker.json") as f:
    youtube_videos_ids = json.load(f)


TEST_URL = "http://localhost:3000/client/youtube/"


async def get_comments(video_id):
    comment_buffer = []
    comments_fetched_cnt = 0

    def finish():
        end = time.time()
        print(f"[FINISHED] {video_id} => {comments_fetched_cnt} / {end - start}s")

    start = time.time()
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                res = await session.get(TEST_URL + video_id + "/page")
                if res.status == 204:
                    time.sleep(0.05)
                elif res.status == 200:
                    res_json = await res.json()
                    comment_buffer.extend(res_json["comments"])
                    comments_fetched_cnt += len(res_json["comments"])
                elif res.status == 205:
                    break
                else:
                    break
            except KeyError:
                print(f"Problem with comment page {video_id}")
                break

    finish()
    return comment_buffer


def worker_download_task(video_id):
    print(f"[START] client: {mp.current_process()} | video: {video_id}")
    comment_buffer = asyncio.run(get_comments(video_id))
    with open(f"../data/comments/{video_id}.json", "w") as f:
        json.dump(comment_buffer, f, ensure_ascii=False)


if __name__ == "__main__":
    print(f"CPU: {mp.cpu_count()}")
    with mp.Pool(processes=mp.cpu_count()) as pool:
        pool.map(worker_download_task, youtube_videos_ids)
