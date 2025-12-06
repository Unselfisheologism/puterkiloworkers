// worker.js - Puter Worker code for OpenAI-compatible Claude API
// Note: Puter Workers auto-load Puter.js, so puter.ai.chat() is available.

// Ignore API key (for private use; accept any Bearer token, including dummy "abcdefg")
router.post('/v1/chat/completions', async ({ request }) => {
  try {
    // Parse request body
    const body = await request.json();
    const { model: clientModel, messages, stream = false, temperature = 0.7, max_tokens } = body;

    const authHeader = request.headers.get('Authorization');
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return new Response(JSON.stringify({ error: { message: 'Invalid API key' } }), {
        status: 401,
        headers: { 'Content-Type': 'application/json' }
      });
    }
    // For private use: Accept dummy key
    const token = authHeader.substring(7);
    if (token !== 'abcdefg') {
      return new Response(JSON.stringify({ error: { message: 'Invalid API key (use abcdefg)' } }), {
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

    // Map OpenAI messages to Claude prompt (XML-style for better multi-turn support)
    const prompt = '<conversation>\n' + 
      messages.map(msg => `<${msg.role}>\n${msg.content}\n</${msg.role}>`).join('\n') + 
      '\n</conversation>\n<assistant>\n';

    // Call Puter AI with Claude Opus 4.5
    const aiResponse = await puter.ai.chat(prompt, {
      model: 'claude-opus-4-5',  // Fixed to requested model
      stream: stream,
      temperature: temperature,
      max_tokens: max_tokens
    });

    // For non-streaming: Get full response
    if (!stream) {
      let fullContent = '';
      if (aiResponse?.message?.content?.[0]?.text) {
        fullContent = aiResponse.message.content[0].text.replace(/^<assistant>\s*/i, '').replace(/\s*<\/assistant>$/i, '').trim();
      } else if (typeof aiResponse === 'string') {
        fullContent = aiResponse;
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
        usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 }  // Placeholder
      };

      return new Response(JSON.stringify(openaiResponse), {
        status: 200,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    // For streaming: Return SSE in OpenAI format
    let accumulatedContent = '';  // Accumulate deltas
    const stream = new ReadableStream({
      async start(controller) {
        try {
          let fullId = `chatcmpl-${Date.now()}`;
          let created = Math.floor(Date.now() / 1000);
          for await (const part of aiResponse) {
            let deltaContent = '';
            if (part?.text) {
              const rawText = part.text.replace(/^<assistant>\s*/i, '').replace(/\s*<\/assistant>$/i, '').trim();
              deltaContent = rawText;  // Full chunk as delta (Claude streams chunks)
              accumulatedContent += deltaContent;
            }
            if (deltaContent) {
              const sseData = `data: ${JSON.stringify({
                id: fullId,
                object: 'chat.completion.chunk',
                created: created,
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
            id: fullId,
            object: 'chat.completion.chunk',
            created: created,
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
