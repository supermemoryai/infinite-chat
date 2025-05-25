import {
  customProvider,
  extractReasoningMiddleware,
  wrapLanguageModel,
} from 'ai';
import { xai } from '@ai-sdk/xai';
import { isTestEnvironment } from '../constants';
import {
  artifactModel,
  chatModel,
  reasoningModel,
  titleModel,
} from './models.test';
import { createOpenAICompatible } from '@ai-sdk/openai-compatible';

if (!process.env.SUPERMEMORY_API_KEY) {
  throw new Error('SUPERMEMORY_API_KEY is not set');
}

const supermemory = (user?: string) => {
  if (!user) {
    throw new Error('User is required');
  }

  return createOpenAICompatible({
    baseURL: 'https://api.supermemory.ai/v3/https://api.openai.com/v1/',
    name: 'supermemory',
    apiKey: process.env.OPENAI_API_KEY,
    headers: {
      // @ts-ignore
      'x-api-key': process.env.SUPERMEMORY_API_KEY,
    },
  }).chatModel('gpt-4o', {
    user: user,
  });
};

export const myProvider = isTestEnvironment
  ? (user?: string) =>
      customProvider({
        languageModels: {
          'chat-model': chatModel,
          'chat-model-reasoning': reasoningModel,
          'title-model': titleModel,
          'artifact-model': artifactModel,
        },
      })
  : (user?: string) =>
      customProvider({
        languageModels: {
          // 'chat-model': xai('grok-2-vision-latest'),
          'chat-model': supermemory(user),
          'chat-model-reasoning': wrapLanguageModel({
            model: xai('grok-3-mini-beta'),
            middleware: extractReasoningMiddleware({ tagName: 'think' }),
          }),
          'title-model': xai('grok-2-1212'),
          'artifact-model': xai('grok-2-1212'),
        },
        imageModels: {
          'small-model': xai.image('grok-2-image'),
        },
      });
