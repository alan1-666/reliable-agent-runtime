package openaicompat

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"safemarket/agent-runtime/internal/model"
)

var (
	ErrInvalidConfig      = errors.New("invalid OpenAI-compatible model config")
	ErrMalformedStream    = errors.New("malformed model stream")
	ErrIncompleteResponse = errors.New("model response did not complete successfully")
)

type Config struct {
	BaseURL                string
	APIKey                 string
	Model                  string
	MaxTokens              int
	InputMicrosPerMillion  int64
	OutputMicrosPerMillion int64
	ExtraBody              map[string]any
	HTTPClient             *http.Client
}

type Adapter struct {
	endpoint               string
	apiKey                 string
	model                  string
	maxTokens              int
	inputMicrosPerMillion  int64
	outputMicrosPerMillion int64
	extraBody              map[string]any
	client                 *http.Client
}

func New(config Config) (*Adapter, error) {
	baseURL := strings.TrimRight(config.BaseURL, "/")
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || config.APIKey == "" || config.Model == "" {
		return nil, ErrInvalidConfig
	}
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 90 * time.Second}
	}
	return &Adapter{
		endpoint:               baseURL + "/chat/completions",
		apiKey:                 config.APIKey,
		model:                  config.Model,
		maxTokens:              config.MaxTokens,
		inputMicrosPerMillion:  config.InputMicrosPerMillion,
		outputMicrosPerMillion: config.OutputMicrosPerMillion,
		extraBody:              cloneMap(config.ExtraBody),
		client:                 client,
	}, nil
}

func (a *Adapter) Name() string { return "openai-compatible:" + a.model }

func (a *Adapter) Capabilities(context.Context) map[model.Capability]bool {
	return map[model.Capability]bool{
		model.CapabilityStreaming:        true,
		model.CapabilityToolCalling:      true,
		model.CapabilityStructuredOutput: true,
	}
}

func (a *Adapter) Stream(ctx context.Context, request model.Request) (<-chan model.Event, <-chan error) {
	events := make(chan model.Event)
	errs := make(chan error, 1)

	go func() {
		defer close(events)
		defer close(errs)

		body, err := a.requestBody(request)
		if err != nil {
			errs <- err
			return
		}
		httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpoint, bytes.NewReader(body))
		if err != nil {
			errs <- err
			return
		}
		httpRequest.Header.Set("Authorization", "Bearer "+a.apiKey)
		httpRequest.Header.Set("Content-Type", "application/json")
		httpRequest.Header.Set("Accept", "text/event-stream")

		response, err := a.client.Do(httpRequest)
		if err != nil {
			errs <- err
			return
		}
		defer response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			message, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
			errs <- HTTPError{StatusCode: response.StatusCode, Body: strings.TrimSpace(string(message))}
			return
		}

		if err := a.consumeStream(ctx, response.Body, events); err != nil {
			errs <- err
		}
	}()

	return events, errs
}

type HTTPError struct {
	StatusCode int
	Body       string
}

func (e HTTPError) Error() string {
	return fmt.Sprintf("model API returned HTTP %d: %s", e.StatusCode, e.Body)
}

func (e HTTPError) Retryable() bool {
	return e.StatusCode == http.StatusRequestTimeout || e.StatusCode == http.StatusTooManyRequests || e.StatusCode >= 500
}

type requestMessage struct {
	Role       string            `json:"role"`
	Content    string            `json:"content"`
	ToolCallID string            `json:"tool_call_id,omitempty"`
	ToolCalls  []requestToolCall `json:"tool_calls,omitempty"`
}

type requestTool struct {
	Type     string              `json:"type"`
	Function requestToolFunction `json:"function"`
}

type requestToolFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

type requestToolCall struct {
	ID       string                  `json:"id"`
	Type     string                  `json:"type"`
	Function requestToolCallFunction `json:"function"`
}

type requestToolCallFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

func (a *Adapter) requestBody(request model.Request) ([]byte, error) {
	messages := make([]requestMessage, 0, len(request.Messages))
	for _, message := range request.Messages {
		converted := requestMessage{
			Role:       message.Role,
			Content:    message.Content,
			ToolCallID: message.ToolCallID,
		}
		for _, call := range message.ToolCalls {
			converted.ToolCalls = append(converted.ToolCalls, requestToolCall{
				ID:   call.ID,
				Type: "function",
				Function: requestToolCallFunction{
					Name:      call.Name,
					Arguments: string(call.Arguments),
				},
			})
		}
		messages = append(messages, converted)
	}

	tools := make([]requestTool, 0, len(request.Tools))
	for _, definition := range request.Tools {
		tools = append(tools, requestTool{
			Type: "function",
			Function: requestToolFunction{
				Name:        definition.Name,
				Description: definition.Description,
				Parameters:  definition.InputSchema,
			},
		})
	}

	body := cloneMap(a.extraBody)
	body["model"] = a.model
	body["messages"] = messages
	body["stream"] = true
	body["stream_options"] = map[string]bool{"include_usage": true}
	if len(tools) > 0 {
		body["tools"] = tools
		body["tool_choice"] = "auto"
	}
	if a.maxTokens > 0 {
		body["max_tokens"] = a.maxTokens
	}
	return json.Marshal(body)
}

type streamChunk struct {
	Choices []struct {
		Delta struct {
			Content   string `json:"content"`
			ToolCalls []struct {
				Index    int    `json:"index"`
				ID       string `json:"id"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"delta"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
}

type toolCallBuilder struct {
	id        string
	name      string
	arguments strings.Builder
}

func (a *Adapter) consumeStream(ctx context.Context, reader io.Reader, events chan<- model.Event) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64<<10), 4<<20)

	var text strings.Builder
	toolCalls := make(map[int]*toolCallBuilder)
	toolCallsEmitted := false
	completed := false
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, ":") {
			continue
		}
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" {
			break
		}

		var chunk streamChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			return fmt.Errorf("%w: %v", ErrMalformedStream, err)
		}
		if chunk.Usage != nil {
			usage := model.Usage{
				InputTokens:  chunk.Usage.PromptTokens,
				OutputTokens: chunk.Usage.CompletionTokens,
			}
			usage.CostMicros = costMicros(usage, a.inputMicrosPerMillion, a.outputMicrosPerMillion)
			if err := send(ctx, events, model.Event{Type: model.EventUsage, Usage: usage}); err != nil {
				return err
			}
		}

		for _, choice := range chunk.Choices {
			if choice.Delta.Content != "" {
				text.WriteString(choice.Delta.Content)
				if err := send(ctx, events, model.Event{Type: model.EventTextDelta, Delta: choice.Delta.Content}); err != nil {
					return err
				}
			}
			for _, delta := range choice.Delta.ToolCalls {
				builder := toolCalls[delta.Index]
				if builder == nil {
					builder = &toolCallBuilder{}
					toolCalls[delta.Index] = builder
				}
				if delta.ID != "" {
					builder.id = delta.ID
				}
				if delta.Function.Name != "" {
					builder.name = delta.Function.Name
				}
				builder.arguments.WriteString(delta.Function.Arguments)
			}

			switch choice.FinishReason {
			case "tool_calls":
				if err := emitToolCalls(ctx, events, toolCalls); err != nil {
					return err
				}
				toolCallsEmitted = true
				completed = true
			case "stop":
				if err := send(ctx, events, model.Event{Type: model.EventCompleted, Output: text.String()}); err != nil {
					return err
				}
				completed = true
			case "length", "content_filter", "insufficient_system_resource", "aborted":
				return fmt.Errorf("%w: finish_reason=%s", ErrIncompleteResponse, choice.FinishReason)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if len(toolCalls) > 0 && !toolCallsEmitted {
		if err := emitToolCalls(ctx, events, toolCalls); err != nil {
			return err
		}
		completed = true
	}
	if !completed {
		return ErrIncompleteResponse
	}
	return nil
}

func emitToolCalls(ctx context.Context, events chan<- model.Event, builders map[int]*toolCallBuilder) error {
	for index := 0; index < len(builders); index++ {
		builder := builders[index]
		if builder == nil || builder.id == "" || builder.name == "" {
			return fmt.Errorf("%w: incomplete tool call at index %d", ErrMalformedStream, index)
		}
		if err := send(ctx, events, model.Event{
			Type: model.EventToolCall,
			ToolCall: model.ToolCall{
				ID:        builder.id,
				Name:      builder.name,
				Arguments: json.RawMessage(builder.arguments.String()),
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

func send(ctx context.Context, events chan<- model.Event, event model.Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case events <- event:
		return nil
	}
}

func costMicros(usage model.Usage, inputMicrosPerMillion, outputMicrosPerMillion int64) int64 {
	input := int64(usage.InputTokens) * inputMicrosPerMillion / 1_000_000
	output := int64(usage.OutputTokens) * outputMicrosPerMillion / 1_000_000
	return input + output
}

func cloneMap(source map[string]any) map[string]any {
	result := make(map[string]any, len(source)+4)
	for key, value := range source {
		result[key] = value
	}
	return result
}
