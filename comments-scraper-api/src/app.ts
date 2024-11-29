import express from "express";
import dotenv from "dotenv";
import asyncHandler from "express-async-handler";
import axios from "axios";

type VideoSession = {
  status: string | undefined;
  nextPageToken: string | undefined;
};

dotenv.config();
let activeSessions = new Map<string, VideoSession>();

function handleApiError(error, response) {
  if (error.response) {
    response.status(error.response.data.error.code);
    response.statusMessage = error.response.data.error.message;
    response.end();
  } else response.send("[Error] Unkown request");
}

function queueCommentPage(commentPage, videoId) {
  activeSessions = activeSessions.set(videoId, {
    status: "ready",
    nextPageToken: commentPage.data.nextPageToken
      ? commentPage.data.nextPageToken
      : "",
  });
}

function extractComments(commentPage) {
  return commentPage.data.items.map((comment) => {
    return {
      author: comment.snippet.topLevelComment.snippet.authorDisplayName,
      text: comment.snippet.topLevelComment.snippet.textOriginal,
      likes: comment.snippet.topLevelComment.snippet.likeCount,
    };
  });
}

const clientGetCommentPage = asyncHandler(async (req, res, next) => {
  const clientVideo = activeSessions.get(req.params.videoId);
  if (!clientVideo) {
    console.log(
      `Active ${activeSessions.size} | [${req.params.videoId}] - [0] New Entry`
    );
    try {
      let commentPage = await axios.get(
        "https://www.googleapis.com/youtube/v3/commentThreads",
        {
          params: {
            part: "snippet",
            videoId: req.params.videoId,
            key: process.env.YOUTUBE_API_TOKEN,
            maxResults: 100,
          },
        }
      );
      queueCommentPage(commentPage, req.params.videoId);
      res.json({
        id: req.params.videoId,
        nextPageToken: commentPage.data.nextPageToken ?? "",
        comments: extractComments(commentPage),
      });
    } catch (error) {
      handleApiError(error, res);
    }
  } else if (clientVideo.status === "active") {
    // if request to youtube api is in progress
    console.log(
      `Active ${activeSessions.size} | [${req.params.videoId}] - [1], No Content`
    );
    res.status(204).send();
  } else if (clientVideo.status === "ready" && clientVideo.nextPageToken) {
    // if request to youtube api is ready to be send
    console.log(
      `Active ${activeSessions.size} | [${req.params.videoId}] - [2], Content`
    );
    clientVideo.status = "active";
    activeSessions = activeSessions.set(req.params.videoId, clientVideo);

    let commentPage = undefined;
    try {
      commentPage = await axios.get(
        "https://www.googleapis.com/youtube/v3/commentThreads",
        {
          params: {
            part: "snippet",
            videoId: req.params.videoId,
            key: process.env.YOUTUBE_API_TOKEN,
            maxResults: 100,
            pageToken: clientVideo.nextPageToken,
          },
        }
      );
      queueCommentPage(commentPage, req.params.videoId);
      res.json({
        id: req.params.videoId,
        nextPageToken: commentPage.data.nextPageToken ?? "",
        comments: extractComments(commentPage),
      });
    } catch (error) {
      handleApiError(error, res);
      activeSessions.delete(req.params.videoId);
    }
  } else if (clientVideo.status === "ready" && !clientVideo.nextPageToken) {
    // if request to youtube api is no longer valid since we reached end of comment page
    console.log(
      `Active ${activeSessions.size} | [${req.params.videoId}] - [3], Reset Content`
    );
    activeSessions.delete(req.params.videoId);
    res.status(205).json({
      id: req.params.videoId,
      commentPage: "no more comment pages",
    });
  }
});

const router = express.Router();
router.get("/youtube/:videoId/page", clientGetCommentPage);

const app = express();
app.use(express.json());
app.use("/client", router);

const port = process.env.PORT;
app.listen(port, () => {
  console.log(`Example app listening on port ${port}`);
});
