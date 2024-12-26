declare global {
    namespace NodeJS {
      interface ProcessEnv {
        YOUTUBE_API_TOKEN: string;
      }
    }
  }
  
  export {};
  