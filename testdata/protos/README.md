### Proto Descriptor Generation

The order.pb file was generated with the following command:

```shell
protoc --include_imports --descriptor_set_out=order.pb --go_out=. order.proto
```
