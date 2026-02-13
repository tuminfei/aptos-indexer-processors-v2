# Hasura 安装和配置指南

## 解决方案

### 步骤1：停止并重新启动Hasura容器，设置正确的环境变量

```bash
# 停止并移除当前容器
docker stop hasura && docker rm hasura

# 重新启动容器，设置必要的环境变量
docker run -d \
  --name hasura \
  -p 10000:8080 \
  -e HASURA_GRAPHQL_DATABASE_URL=postgresql://postgres:@host.docker.internal:5433/indexer_v2 \
  -e INDEXER_V2_POSTGRES_URL=postgresql://postgres:@host.docker.internal:5433/indexer_v2 \
  -e HASURA_GRAPHQL_ENABLE_CONSOLE=true \
  -e HASURA_GRAPHQL_DEV_MODE=true \
  hasura/graphql-engine:latest
```

### 步骤2：确保processor已经运行并创建了必要的表结构

```bash
# 进入processor目录
cd /Volumes/Data_dev/Documents/Codes/Rust/aptos-indexer-processors-v2/processor

# 运行processor（如果尚未运行）
cargo run --release -- -c config.yaml
```

### 步骤3：重新加载元数据

```bash
# 进入hasura-api目录
cd /Volumes/Data_dev/Documents/Codes/Rust/aptos-indexer-processors-v2/hasura-api

# 使用curl命令通过API加载元数据
curl -X POST http://localhost:10000/v1/metadata \
  -H "Content-Type: application/json" \
  -d "{
    \"type\": \"replace_metadata\",
    \"args\": $(cat metadata-json/unified.json)
  }"
```

### 步骤4：验证元数据加载成功

1. **访问Hasura控制台**：
   ```
   http://localhost:10000/console
   ```

2. **检查表结构**：
   - 在控制台中，查看"Data"标签页，确认是否能看到processor创建的表结构
   - 特别是确认 `processor_status` 表是否存在

## 注意事项

1. **processor运行**：确保processor已经运行了一段时间，这样它才能创建必要的表结构
2. **数据库连接**：确保Hasura和processor使用的是同一个数据库
3. **环境变量**：确保设置了所有必要的环境变量，特别是 `INDEXER_V2_POSTGRES_URL`

通过以上步骤，您应该能够成功加载元数据并开始使用Hasura的GraphQL API来查询processor索引的数据。
