import type Anthropic from '@anthropic-ai/sdk'
import type { LLMMessage, LLMToolResultBlock } from '../handlers/llm/types'
import { expect, it } from 'vitest'
import { assertValidAnthropicToolUseContract } from '../handlers/llm/anthropicContract'
import { encodeAnthropicRequestMessages } from '../handlers/llm/conversationCodec'
import { anthropicStateStrategy } from '../handlers/llm/stateStrategy'
import { transformMessages } from '../handlers/llm/transformMessages'

it('accepts tool_result-first combined user message after tool_use', () => {
  const messages: Anthropic.MessageParam[] = [
    {
      role: 'assistant',
      content: [
        { type: 'text', text: '我先查一下。' },
        { type: 'tool_use', id: 'tool-1', name: 'Read', input: { path: 'a.md' } },
      ],
    },
    {
      role: 'user',
      content: [
        { type: 'tool_result', tool_use_id: 'tool-1', content: 'ok' },
        { type: 'text', text: '继续' },
      ],
    },
  ]

  expect(() => assertValidAnthropicToolUseContract(messages)).not.toThrow()
})

it('rejects assistant text after tool_use', () => {
  const messages: Anthropic.MessageParam[] = [
    {
      role: 'assistant',
      content: [
        { type: 'tool_use', id: 'tool-1', name: 'Read', input: { path: 'a.md' } },
        { type: 'text', text: '这段文本不应出现在 tool_use 后' },
      ],
    },
    {
      role: 'user',
      content: [{ type: 'tool_result', tool_use_id: 'tool-1', content: 'ok' }],
    },
  ]

  expect(() => assertValidAnthropicToolUseContract(messages)).toThrow(/contains text after tool_use/)
})

it('rejects non-tool_result content appearing before a required tool_result prefix', () => {
  const messages: Anthropic.MessageParam[] = [
    {
      role: 'assistant',
      content: [
        { type: 'tool_use', id: 'tool-1', name: 'Read', input: { path: 'a.md' } },
      ],
    },
    {
      role: 'user',
      content: [
        { type: 'text', text: '我先说一句话' },
        { type: 'tool_result', tool_use_id: 'tool-1', content: 'ok' },
      ],
    },
  ]

  expect(() => assertValidAnthropicToolUseContract(messages)).toThrow(/contains tool_result after non-tool_result content/)
})

it('rejects out-of-order tool_result prefix in next user message', () => {
  const messages: Anthropic.MessageParam[] = [
    {
      role: 'assistant',
      content: [
        { type: 'tool_use', id: 'tool-A', name: 'Read', input: { path: 'a.md' } },
        { type: 'tool_use', id: 'tool-B', name: 'Grep', input: { path: '.' } },
      ],
    },
    {
      role: 'user',
      content: [
        { type: 'tool_result', tool_use_id: 'tool-B', content: 'grep ok' },
        { type: 'tool_result', tool_use_id: 'tool-A', content: 'read ok' },
      ],
    },
  ]

  expect(() => assertValidAnthropicToolUseContract(messages)).toThrow(/tool_result order mismatch/)
})

it('accepts multiple tool_results followed by user text when the tool_result prefix matches tool_use order', () => {
  const messages: Anthropic.MessageParam[] = [
    {
      role: 'assistant',
      content: [
        { type: 'text', text: '先跑两个工具。' },
        { type: 'tool_use', id: 'tool-A', name: 'Read', input: { path: 'a.md' } },
        { type: 'tool_use', id: 'tool-B', name: 'Grep', input: { path: '.' } },
      ],
    },
    {
      role: 'user',
      content: [
        { type: 'tool_result', tool_use_id: 'tool-A', content: 'read ok' },
        { type: 'tool_result', tool_use_id: 'tool-B', content: 'grep ok' },
        { type: 'text', text: '继续分析这些结果' },
      ],
    },
  ]

  expect(() => assertValidAnthropicToolUseContract(messages)).not.toThrow()
})

it('anthropic state strategy keeps canonical tool messages in tool_use order, and anthropic compilation still satisfies contract', () => {
  const messages: LLMMessage[] = [
    {
      role: 'assistant',
      content: [
        { type: 'text', text: '先跑两个工具。' },
        { type: 'tool_use', id: 'tool-A', name: 'Read', input: { path: 'a.md' } },
        { type: 'tool_use', id: 'tool-B', name: 'Grep', input: { path: '.' } },
      ],
    },
  ]
  const pending: LLMToolResultBlock[] = [
    anthropicStateStrategy.createToolResult({
      toolCallId: 'tool-B',
      toolName: 'Grep',
      content: 'grep ok',
      isError: false,
    }),
    anthropicStateStrategy.createToolResult({
      toolCallId: 'tool-A',
      toolName: 'Read',
      content: 'read ok',
      isError: false,
    }),
  ]

  anthropicStateStrategy.flushToolResults(messages, pending)

  expect(pending).toEqual([])
  expect(messages.slice(1)).toEqual([
    { role: 'tool', toolCallId: 'tool-A', toolName: 'Read', content: 'read ok' },
    { role: 'tool', toolCallId: 'tool-B', toolName: 'Grep', content: 'grep ok' },
  ])

  const compiled = transformMessages(messages, 'anthropic')
  const encoded = encodeAnthropicRequestMessages(compiled)
  expect(() => assertValidAnthropicToolUseContract(encoded.messages)).not.toThrow()
})

it('hoists downgraded unsupported tool calls before the surviving tool_use blocks', () => {
  const messages: LLMMessage[] = [
    { role: 'user', content: 'go' },
    {
      role: 'assistant',
      content: [
        { type: 'tool_use', id: 'call_edit', name: 'Edit', input: { path: 'a.ts' } },
        { type: 'tool_use', id: 'call_patch', name: 'ApplyPatch', input: { patch: 'x' } },
      ],
    },
    { role: 'tool', toolCallId: 'call_edit', toolName: 'Edit', content: 'edit ok' },
    { role: 'tool', toolCallId: 'call_patch', toolName: 'ApplyPatch', content: 'patch ok' },
    { role: 'user', content: '继续' },
  ]

  const compiled = transformMessages(messages, 'anthropic')
  const encoded = encodeAnthropicRequestMessages(compiled)
  expect(() => assertValidAnthropicToolUseContract(encoded.messages)).not.toThrow()

  const assistant = encoded.messages.find(message => message.role === 'assistant')
  const blockTypes = (assistant?.content as Anthropic.ContentBlockParam[]).map(block => block.type)
  expect(blockTypes).toEqual(['text', 'tool_use'])
})
