import {
  appendClientMessage,
  appendResponseMessages,
  createDataStream,
  smoothStream,
  streamText,
} from 'ai';
import { auth, type UserType } from '@/app/(auth)/auth';
import { type RequestHints, systemPrompt } from '@/lib/ai/prompts';
import {
  createStreamId,
  deleteChatById,
  getChatById,
  getMessageCountByUserId,
  getMessagesByChatId,
  getStreamIdsByChatId,
  saveChat,
  saveMessages,
} from '@/lib/db/queries';
import { generateUUID, getTrailingMessageId } from '@/lib/utils';
import { generateTitleFromUserMessage } from '../../actions';
import { createDocument } from '@/lib/ai/tools/create-document';
import { updateDocument } from '@/lib/ai/tools/update-document';
import { requestSuggestions } from '@/lib/ai/tools/request-suggestions';
import { getWeather } from '@/lib/ai/tools/get-weather';
import { isProductionEnvironment } from '@/lib/constants';
import { myProvider } from '@/lib/ai/providers';
import { entitlementsByUserType } from '@/lib/ai/entitlements';
import { postRequestBodySchema, type PostRequestBody } from './schema';
import { geolocation } from '@vercel/functions';
import {
  createResumableStreamContext,
  type ResumableStreamContext,
} from 'resumable-stream';
import { after } from 'next/server';
import type { Chat } from '@/lib/db/schema';
import { differenceInSeconds } from 'date-fns';
import { ChatSDKError } from '@/lib/errors';

export const maxDuration = 60;

let globalStreamContext: ResumableStreamContext | null = null;

function getStreamContext() {
  console.log('[getStreamContext] Called');
  if (!globalStreamContext) {
    console.log(
      '[getStreamContext] No global stream context, creating new one',
    );
    try {
      globalStreamContext = createResumableStreamContext({
        waitUntil: after,
      });
      console.log('[getStreamContext] Successfully created stream context');
    } catch (error: any) {
      console.log(
        '[getStreamContext] Error creating stream context:',
        error.message,
      );
      if (error.message.includes('REDIS_URL')) {
        console.log(
          ' > Resumable streams are disabled due to missing REDIS_URL',
        );
      } else {
        console.error(error);
      }
    }
  } else {
    console.log('[getStreamContext] Using existing global stream context');
  }

  return globalStreamContext;
}

export async function POST(request: Request) {
  console.log('[POST] Chat API called');
  let requestBody: PostRequestBody;

  try {
    console.log('[POST] Parsing request JSON');
    const json = await request.json();
    console.log('[POST] Raw request JSON:', json);
    requestBody = postRequestBodySchema.parse(json);
    console.log('[POST] Validated request body:', requestBody);
  } catch (e) {
    console.log('[POST] Error parsing request body:', e);
    return new ChatSDKError('bad_request:api', String(e)).toResponse();
  }

  try {
    const { id, message, selectedChatModel, selectedVisibilityType } =
      requestBody;
    console.log('[POST] Extracted request data:', {
      id,
      messageId: message.id,
      selectedChatModel,
      selectedVisibilityType,
    });

    console.log('[POST] Getting user session');
    const session = await auth();
    console.log('[POST] Session retrieved:', {
      userId: session?.user?.id,
      userType: session?.user?.type,
    });

    if (!session?.user) {
      console.log('[POST] No user session found, returning unauthorized');
      return new ChatSDKError('unauthorized:chat').toResponse();
    }

    const userType: UserType = session.user.type;
    console.log('[POST] User type:', userType);

    console.log('[POST] Checking message count for rate limiting');
    const messageCount = await getMessageCountByUserId({
      id: session.user.id,
      differenceInHours: 24,
    });
    console.log('[POST] User message count in last 24h:', messageCount);
    console.log(
      '[POST] Max allowed messages for user type:',
      entitlementsByUserType[userType].maxMessagesPerDay,
    );

    if (messageCount > entitlementsByUserType[userType].maxMessagesPerDay) {
      console.log('[POST] Rate limit exceeded, returning error');
      return new ChatSDKError('rate_limit:chat').toResponse();
    }

    console.log('[POST] Getting chat by ID:', id);
    const chat = await getChatById({ id });
    console.log(
      '[POST] Chat retrieved:',
      chat ? { id: chat.id, userId: chat.userId, title: chat.title } : 'null',
    );

    if (!chat) {
      console.log('[POST] Chat not found, creating new chat');
      // const title = await generateTitleFromUserMessage({
      //   message,
      //   userId: session.user.id,
      // });
      // console.log('[POST] Generated title:', title);

      const title = '';

      await saveChat({
        id,
        userId: session.user.id,
        title,
        visibility: selectedVisibilityType,
      });
      console.log('[POST] New chat saved successfully');
    } else {
      console.log('[POST] Chat exists, checking ownership');
      if (chat.userId !== session.user.id) {
        console.log('[POST] User does not own this chat, returning forbidden');
        return new ChatSDKError('forbidden:chat').toResponse();
      }
      console.log('[POST] User owns the chat, proceeding');
    }

    console.log('[POST] Getting previous messages for chat');
    const previousMessages = await getMessagesByChatId({ id });
    console.log('[POST] Previous messages count:', previousMessages.length);

    console.log('[POST] Appending client message to conversation');
    const messages = appendClientMessage({
      // @ts-expect-error: todo add type conversion from DBMessage[] to UIMessage[]
      messages: previousMessages,
      message,
    });
    console.log('[POST] Total messages after appending:', messages.length);

    console.log('[POST] Getting geolocation from request');
    const { longitude, latitude, city, country } = geolocation(request);
    console.log('[POST] Geolocation data:', {
      longitude,
      latitude,
      city,
      country,
    });

    const requestHints: RequestHints = {
      longitude,
      latitude,
      city,
      country,
    };

    console.log('[POST] Saving user message to database');
    await saveMessages({
      messages: [
        {
          chatId: id,
          id: message.id,
          role: 'user',
          parts: message.parts,
          attachments: message.experimental_attachments ?? [],
          createdAt: new Date(),
        },
      ],
    });
    console.log('[POST] User message saved successfully');

    console.log('[POST] Generating stream ID');
    const streamId = generateUUID();
    console.log('[POST] Generated stream ID:', streamId);
    await createStreamId({ streamId, chatId: id });
    console.log('[POST] Stream ID saved to database');

    console.log('[POST] Creating data stream for AI response');
    const stream = createDataStream({
      execute: (dataStream) => {
        console.log('[POST] [execute] Data stream execution started');
        console.log('[POST] [execute] Selected chat model:', selectedChatModel);
        console.log(
          '[POST] [execute] Active tools check - is reasoning model?',
          selectedChatModel === 'chat-model-reasoning',
        );

        const result = streamText({
          model: myProvider(session.user.id).languageModel(selectedChatModel),
          system: systemPrompt({ selectedChatModel, requestHints }),
          messages,
          maxSteps: 5,
          experimental_activeTools:
            selectedChatModel === 'chat-model-reasoning'
              ? []
              : [
                  'getWeather',
                  'createDocument',
                  'updateDocument',
                  'requestSuggestions',
                ],
          experimental_transform: smoothStream({ chunking: 'word' }),
          experimental_generateMessageId: generateUUID,
          tools: {
            getWeather,
            createDocument: createDocument({ session, dataStream }),
            updateDocument: updateDocument({ session, dataStream }),
            requestSuggestions: requestSuggestions({
              session,
              dataStream,
            }),
          },
          onFinish: async ({ response, usage }) => {
            console.log(
              '[POST] [onFinish] Stream finished, processing response',
            );
            console.log('[POST] [onFinish] Usage:', usage);
            console.log(
              '[POST] [onFinish] Response messages count:',
              response.messages.length,
            );

            if (session.user?.id) {
              console.log(
                '[POST] [onFinish] User session exists, saving assistant message',
              );
              try {
                console.log('[POST] [onFinish] Appending response messages');
                const allResponseMessages = appendResponseMessages({
                  messages: [message],
                  responseMessages: response.messages,
                });
                // Skip index 0 (the original user message) and save ALL
                // response messages so that multi-step tool_use / tool_result
                // pairs are fully persisted and the conversation history sent
                // to Anthropic remains valid.
                const messagesToSave = allResponseMessages.slice(1).map(
                  (msg) => ({
                    id: msg.id,
                    chatId: id,
                    role: msg.role,
                    parts: msg.parts,
                    attachments: msg.experimental_attachments ?? [],
                    createdAt: new Date(),
                  }),
                );
                console.log('[POST] [onFinish] Messages to save:', {
                  count: messagesToSave.length,
                  roles: messagesToSave.map((m) => m.role),
                });

                console.log(
                  '[POST] [onFinish] Saving all response messages to database',
                );
                await saveMessages({ messages: messagesToSave });
                console.log(
                  '[POST] [onFinish] All response messages saved successfully',
                );

                // Disabled message eviction for now
                // // Check token usage and delete oldest message if needed
                // const totalTokens = usage.totalTokens || 0;
                // console.log('totalTokens', totalTokens);
                // if (totalTokens > 10000) {
                //   console.log(
                //     `Token usage (${totalTokens}) exceeds limit. Deleting oldest message.`,
                //   );
                //   await deleteOldestMessageInChat({ chatId: id });
                // }
              } catch (error) {
                console.error('[POST] [onFinish] Failed to save chat:', error);
              }
            } else {
              console.log(
                '[POST] [onFinish] No user session, skipping message save',
              );
            }
          },
          experimental_telemetry: {
            isEnabled: isProductionEnvironment,
            functionId: 'stream-text',
          },
        });

        console.log('[POST] [execute] Consuming stream');
        result.consumeStream();

        console.log('[POST] [execute] Merging into data stream');
        result.mergeIntoDataStream(dataStream, {
          sendReasoning: true,
        });
      },
      onError: (e) => {
        console.error('[POST] [onError] Data stream error:', e);
        return 'Oops, an error occurred!';
      },
    });

    console.log('[POST] Getting stream context for resumable streams');
    const streamContext = getStreamContext();

    if (streamContext) {
      console.log('[POST] Stream context available, creating resumable stream');
      return new Response(
        await streamContext.resumableStream(streamId, () => stream),
      );
    } else {
      console.log('[POST] No stream context, returning regular stream');
      return new Response(stream);
    }
  } catch (error) {
    console.error('[POST] Top-level error caught:', error);
    if (error instanceof ChatSDKError) {
      console.log('[POST] Returning ChatSDKError response');
      return error.toResponse();
    }
    console.log('[POST] Unexpected error type, re-throwing');
    throw error;
  }
}

export async function GET(request: Request) {
  console.log('[GET] Resume stream API called');
  const streamContext = getStreamContext();
  const resumeRequestedAt = new Date();
  console.log('[GET] Resume requested at:', resumeRequestedAt.toISOString());

  if (!streamContext) {
    console.log('[GET] No stream context available, returning 204');
    return new Response(null, { status: 204 });
  }

  console.log('[GET] Parsing request URL');
  const { searchParams } = new URL(request.url);
  const chatId = searchParams.get('chatId');
  console.log('[GET] Chat ID from params:', chatId);

  if (!chatId) {
    console.log('[GET] No chat ID provided, returning bad request');
    return new ChatSDKError('bad_request:api').toResponse();
  }

  console.log('[GET] Getting user session');
  const session = await auth();
  console.log('[GET] Session retrieved:', { userId: session?.user?.id });

  if (!session?.user) {
    console.log('[GET] No user session found, returning unauthorized');
    return new ChatSDKError('unauthorized:chat').toResponse();
  }

  let chat: Chat;

  try {
    console.log('[GET] Getting chat by ID');
    chat = await getChatById({ id: chatId });
    console.log('[GET] Chat retrieved:', {
      id: chat.id,
      userId: chat.userId,
      visibility: chat.visibility,
    });
  } catch (error) {
    console.log('[GET] Error getting chat:', error);
    return new ChatSDKError('not_found:chat').toResponse();
  }

  if (!chat) {
    console.log('[GET] Chat not found');
    return new ChatSDKError('not_found:chat').toResponse();
  }

  console.log('[GET] Checking chat access permissions');
  if (chat.visibility === 'private' && chat.userId !== session.user.id) {
    console.log(
      '[GET] Private chat, user does not own it, returning forbidden',
    );
    return new ChatSDKError('forbidden:chat').toResponse();
  }
  console.log('[GET] User has access to chat');

  console.log('[GET] Getting stream IDs for chat');
  const streamIds = await getStreamIdsByChatId({ chatId });
  console.log('[GET] Stream IDs found:', streamIds.length);

  if (!streamIds.length) {
    console.log('[GET] No stream IDs found');
    return new ChatSDKError('not_found:stream').toResponse();
  }

  const recentStreamId = streamIds.at(-1);
  console.log('[GET] Most recent stream ID:', recentStreamId);

  if (!recentStreamId) {
    console.log('[GET] No recent stream ID found');
    return new ChatSDKError('not_found:stream').toResponse();
  }

  console.log('[GET] Creating empty data stream');
  const emptyDataStream = createDataStream({
    execute: () => {
      console.log('[GET] [execute] Empty data stream executed');
    },
  });

  console.log('[GET] Attempting to resume stream');
  const stream = await streamContext.resumableStream(
    recentStreamId,
    () => emptyDataStream,
  );

  /*
   * For when the generation is streaming during SSR
   * but the resumable stream has concluded at this point.
   */
  if (!stream) {
    console.log('[GET] No active stream found, checking for recent messages');
    const messages = await getMessagesByChatId({ id: chatId });
    console.log('[GET] Messages retrieved:', messages.length);
    const mostRecentMessage = messages.at(-1);

    if (!mostRecentMessage) {
      console.log('[GET] No recent message found, returning empty stream');
      return new Response(emptyDataStream, { status: 200 });
    }

    console.log('[GET] Most recent message role:', mostRecentMessage.role);
    if (mostRecentMessage.role !== 'assistant') {
      console.log(
        '[GET] Most recent message is not from assistant, returning empty stream',
      );
      return new Response(emptyDataStream, { status: 200 });
    }

    const messageCreatedAt = new Date(mostRecentMessage.createdAt);
    const timeDiff = differenceInSeconds(resumeRequestedAt, messageCreatedAt);
    console.log('[GET] Message age in seconds:', timeDiff);

    if (timeDiff > 15) {
      console.log('[GET] Message is too old (>15s), returning empty stream');
      return new Response(emptyDataStream, { status: 200 });
    }

    console.log('[GET] Creating restored stream with recent message');
    const restoredStream = createDataStream({
      execute: (buffer) => {
        console.log('[GET] [execute] Writing restored message to buffer');
        buffer.writeData({
          type: 'append-message',
          message: JSON.stringify(mostRecentMessage),
        });
      },
    });

    return new Response(restoredStream, { status: 200 });
  }

  console.log('[GET] Returning active stream');
  return new Response(stream, { status: 200 });
}

export async function DELETE(request: Request) {
  console.log('[DELETE] Delete chat API called');
  const { searchParams } = new URL(request.url);
  const id = searchParams.get('id');
  console.log('[DELETE] Chat ID to delete:', id);

  if (!id) {
    console.log('[DELETE] No chat ID provided, returning bad request');
    return new ChatSDKError('bad_request:api').toResponse();
  }

  console.log('[DELETE] Getting user session');
  const session = await auth();
  console.log('[DELETE] Session retrieved:', { userId: session?.user?.id });

  if (!session?.user) {
    console.log('[DELETE] No user session found, returning unauthorized');
    return new ChatSDKError('unauthorized:chat').toResponse();
  }

  console.log('[DELETE] Getting chat by ID for ownership check');
  const chat = await getChatById({ id });
  console.log('[DELETE] Chat retrieved:', { id: chat.id, userId: chat.userId });

  if (chat.userId !== session.user.id) {
    console.log('[DELETE] User does not own this chat, returning forbidden');
    return new ChatSDKError('forbidden:chat').toResponse();
  }

  console.log('[DELETE] Deleting chat');
  const deletedChat = await deleteChatById({ id });
  console.log('[DELETE] Chat deleted successfully:', {
    deletedId: deletedChat.id,
  });

  return Response.json(deletedChat, { status: 200 });
}
