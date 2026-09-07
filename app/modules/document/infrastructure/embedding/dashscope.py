"""通过 DashScope OpenAI 兼容接口生成文本 Embedding 向量的服务实现。

封装 Qwen / DashScope Embedding 服务的客户端初始化、按 EMBEDDING_BATCH_SIZE
分批调用与向量数组提取。
"""

from openai import OpenAI

from app.config.settings import (
    DASHSCOPE_API_KEY,
    DASHSCOPE_BASE_URL,
    EMBEDDING_MODEL_NAME,
    EMBEDDING_BATCH_SIZE,
)


class EmbeddingService:
    """封装 DashScope Qwen Embedding 远程调用的客户端服务。"""

    def __init__(self) -> None:
        """校验配置并初始化 OpenAI-compatible HTTP 客户端。

        Raises:
            RuntimeError: 当 DASHSCOPE_API_KEY 未配置时抛出。
        """
        if not DASHSCOPE_API_KEY:
            raise RuntimeError("DASHSCOPE_API_KEY 未配置")

        self.client = OpenAI(
            api_key=DASHSCOPE_API_KEY,
            base_url=DASHSCOPE_BASE_URL,
        )

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        """为输入的多条文本生成高维浮点数稠密向量列表。

        内部按 EMBEDDING_BATCH_SIZE 进行切批请求，保证调用不超过服务商批量上限。

        Args:
            texts: 待向量化的文本字符串列表（如 child_chunk 的 embedding_text）。

        Returns:
            与输入文本顺序一一对应的浮点数向量列表。
        """
        if not texts:
            return []

        all_vectors: list[list[float]] = []
        for batch in self._batch_texts(texts):
            # 调用 DashScope OpenAI-compatible 接口
            response = self.client.embeddings.create(
                model=EMBEDDING_MODEL_NAME,
                input=batch,
            )

            vectors = [item.embedding for item in response.data]
            all_vectors.extend(vectors)
        return all_vectors

    def _batch_texts(self, texts: list[str]) -> list[list[str]]:
        """按应用配置的 EMBEDDING_BATCH_SIZE 将文本列表切分为批次列表。"""
        return [
            texts[i:i + EMBEDDING_BATCH_SIZE]
            for i in range(0, len(texts), EMBEDDING_BATCH_SIZE)
        ]
