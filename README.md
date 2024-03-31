# NeuroTools

一些帮助我切片的小工具

## check_chats.py

用于检测Chat中的敏感词

### 安装

```shell
poetry install --only check_chats
```

### 示例

```shell
poetry run python check_chats.py --video_id 2096439875 --start_time 01:48:40 --end_time 01:53:40 --keywords 台湾
```