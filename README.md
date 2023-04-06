
# GPT Line Protocol

A human-readable line protocol generated by ChatGPT, which has comparable performance to protobuf, and much better performance than prototext/protojson.

The protocol is designed to be human-readable, option encoding passed by to remote service, and easy to parse.

I wrote a fuzzer to ensure the correctness but is still possible to have bugs. I will not be responsible for any data loss.

+ `line_protocol.Encoder` is for text encoding.

+ `line_protocol.Decoder` is for text decoding.

+ `line_protocol.Query` is for random access to line options.

### Generation

The README is also generated by ChatGPT.

