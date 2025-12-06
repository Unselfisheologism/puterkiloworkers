// worker.js - Puter Worker code for OpenAI-compatible Claude API
router.post('/v1/chat/completions', async ({ request }) => {
  try {
    // Parse request body
    const body = await request.json();
    const { model: clientModel, messages, stream = false, temperature = 0.7, max_tokens } = body;

    // Ignore API key (for private use; always "authorize")
    const authHeader = request.headers.get('Authorization');
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return new Response(JSON.stringify({ error: { message: 'Invalid API key' } }), {
        status: 401,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    // Validate required fields
    if (!messages || !Array.isArray(messages) || messages.length === 0) {
      return new Response(JSON.stringify({ error: { message: 'Messages array required' } }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    // Map OpenAI messages to Claude prompt (concatenate for simplicity; enhance for multi-turn if needed)
    const prompt = messages.map(msg => `${msg.role === 'user' ? 'User' : 'Assistant'}: ${msg.content}`).join('\n');

    // Call Puter AI with Claude Opus 4.5 (override client model for now; can add support for others)
    const aiResponse = await puter.ai.chat(prompt, {
      model: 'claude-opus-4-5',  // Fixed to your requested model
      stream: stream,
      temperature: temperature,
      max_tokens: max_tokens
    });

    // For non-streaming: Get full response
    if (!stream) {
      let fullContent = '';
      if (aiResponse.message && aiResponse.message.content && aiResponse.message.content[0]) {
        fullContent = aiResponse.message.content[0].text;
      } else if (typeof aiResponse === 'string') {
        fullContent = aiResponse;  // Fallback if direct string
      }

      const openaiResponse = {
        id: `chatcmpl-${Date.now()}`,
        object: 'chat.completion',
        created: Math.floor(Date.now() / 1000),
        model: 'claude-opus-4-5',
        choices: [
          {
            index: 0,
            message: {
              role: 'assistant',
              content: fullContent
            },
            finish_reason: 'stop'
          }
        ],
        usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 }  // Placeholder; enhance with real counts if needed
      };

      return new Response(JSON.stringify(openaiResponse), {
        status: 200,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    // For streaming: Return a streaming response (OpenAI format: SSE)
    // Note: Puter.ai.chat stream yields parts with .text; we map to OpenAI delta format
    const stream = new ReadableStream({
      async start(controller) {
        try {
          for await (const part of aiResponse) {
            const deltaContent = part?.text || '';
            if (deltaContent) {
              const sseData = `data: ${JSON.stringify({
                id: `chatcmpl-${Date.now()}`,
                object: 'chat.completion.chunk',
                created: Math.floor(Date.now() / 1000),
                model: 'claude-opus-4-5',
                choices: [{
                  index: 0,
                  delta: { content: deltaContent },
                  finish_reason: null
                }]
              })}\n\n`;
              controller.enqueue(new TextEncoder().encode(sseData));
            }
          }
          // End stream
          const endData = `data: ${JSON.stringify({
            id: `chatcmpl-${Date.now()}`,
            object: 'chat.completion.chunk',
            created: Math.floor(Date.now() / 1000),
            model: 'claude-opus-4-5',
            choices: [{
              index: 0,
              delta: {},
              finish_reason: 'stop'
            }]
          })}\n\n data: [DONE]\n\n`;
          controller.enqueue(new TextEncoder().encode(endData));
        } catch (err) {
          controller.error(err);
        } finally {
          controller.close();
        }
      }
    });

    return new Response(stream, {
      status: 200,
      headers: {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive'
      }
    });

  } catch (error) {
    return new Response(JSON.stringify({ error: { message: error.message } }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }
});
