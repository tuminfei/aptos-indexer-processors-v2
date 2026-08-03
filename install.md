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

docker run -d \
    --name hasura \
    --add-host=host.docker.internal:host-gateway \
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
# 从项目根目录进入 hasura-api 目录
cd hasura-api

# 通过 Metadata API 加载完整配置
jq '{type: "replace_metadata", args: .}' metadata-json/unified.json \
  | curl --fail-with-body -X POST http://localhost:10000/v1/metadata \
      -H 'Content-Type: application/json' \
      --data-binary @-
```

成功时 Hasura 返回：

```json
{"message":"success"}
```

> Fungible Asset 数据迁移期间请将 `metadata-json/unified.json` 替换为
> `metadata-json/unified_transition.json`。如果 Hasura 启用了 Admin Secret，
> 还需要为 `curl` 添加 `-H 'X-Hasura-Admin-Secret: YOUR_ADMIN_SECRET'`。

`replace_metadata` 会替换 Hasura 当前的完整 Metadata。执行前应确认选择了与当前
部署阶段对应的文件。

### 步骤4：配置 `app_registered` Array Relationships

以下两个 Array Relationship 已保存在 `metadata-json/unified.json` 和
`metadata-json/unified_transition.json` 中。完成步骤3后会自动生效，无需再在
Hasura Console 中手工创建。

| Relationship 名称 | From | Target table | To |
| --- | --- | --- | --- |
| `fungible_asset_balances` | `app_registered.equity_token_address` | `current_fungible_asset_balances` | `current_fungible_asset_balances.asset_type` |
| `contribution_events` | `app_registered.app_address` | `contribution_events` | `contribution_events.app_address` |

对应关系如下：

```text
app_registered.equity_token_address
    = current_fungible_asset_balances.asset_type

app_registered.app_address
    = contribution_events.app_address
```

如需在 Hasura Console 中检查或手工配置，请进入：

```text
Data → app_registered → Relationships → Add Relationship
```

两项关系都选择 `Array Relationship` 和 `Manual Configuration`，然后按照上表填写
relationship 名称、target table 和 column mapping。保存后，GraphQL Schema 中的
`app_registered` 将包含：

```text
app_registered
├── fungible_asset_balances[]
└── contribution_events[]
```

### 步骤5：验证元数据加载成功

1. **访问Hasura控制台**：
   ```
   http://localhost:10000/console
   ```

2. **检查表结构**：
   - 在控制台中，查看"Data"标签页，确认是否能看到processor创建的表结构
   - 特别是确认 `processor_status` 表是否存在
   - 打开 `app_registered → Relationships`，确认存在
     `fungible_asset_balances` 和 `contribution_events`

3. **通过 GraphQL 验证关系**：

   ```graphql
   query AppRegisteredRelationships {
     app_registered(limit: 10) {
       app_address
       equity_token_address
       contribution_events {
         contributor
         equity_amount
         period
       }
       fungible_asset_balances {
         asset_type
         owner_address
         amount
       }
     }
   }
   ```

## 注意事项

1. **processor运行**：确保processor已经运行了一段时间，这样它才能创建必要的表结构
2. **数据库连接**：确保Hasura和processor使用的是同一个数据库
3. **环境变量**：确保设置了所有必要的环境变量，特别是 `INDEXER_V2_POSTGRES_URL`

## 更新配置后重启容器

当您需要更新Hasura的配置（例如修改数据库连接字符串、添加新的环境变量等）时，请按照以下步骤操作：

```bash
# 1. 停止并移除当前容器
docker stop hasura && docker rm hasura

# 2. 使用新的配置重新启动容器
docker run -d \
  --name hasura \
  --add-host=host.docker.internal:host-gateway \
  -p 10000:8080 \
  -e HASURA_GRAPHQL_DATABASE_URL=postgresql://postgres:@host.docker.internal:5433/indexer_v2 \
  -e INDEXER_V2_POSTGRES_URL=postgresql://postgres:@host.docker.internal:5433/indexer_v2 \
  -e HASURA_GRAPHQL_ENABLE_CONSOLE=true \
  -e HASURA_GRAPHQL_DEV_MODE=true \
  # 添加其他需要的环境变量
  hasura/graphql-engine:latest

# 3. 从项目根目录进入 hasura-api，并重新加载元数据
cd hasura-api

jq '{type: "replace_metadata", args: .}' metadata-json/unified.json \
  | curl --fail-with-body -X POST http://localhost:10000/v1/metadata \
      -H 'Content-Type: application/json' \
      --data-binary @-
```

通过以上步骤，您应该能够成功加载元数据并开始使用Hasura的GraphQL API来查询processor索引的数据。
