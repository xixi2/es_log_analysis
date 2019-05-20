PRIME_NUMBER = 79


class TrieNode:
    def __init__(self):
        self.path = 0
        self.end = 0
        self.map = [None] * PRIME_NUMBER  # a-z以及几个预留空间留给.等字符


class Trie:
    def __init__(self):
        self.root = TrieNode()

    def insert(self, word):
        if not len(word):
            return
        node = self.root
        node.path += 1
        for i in range(len(word)):
            index = ord(word[i]) % PRIME_NUMBER
            # print("index: %s, word[i]: %s" % (index, word[i]))
            if not node.map[index]:
                node.map[index] = TrieNode()
            node = node.map[index]
            node.path += 1
        node.end += 1

    def search(self, word):
        if not len(word):
            return False
        node = self.root
        for i in range(len(word)):
            # index = ord(word[i]) - ord("a")
            index = ord(word[i]) % PRIME_NUMBER
            if not node.map[index]:
                # print("index: %s, word[i]: %s" % (index, word[i]))
                node.map[index] = TrieNode()
            node = node.map[index]
        return node.end != 0


if __name__ == "__main__":
    trie = Trie()
    words = ["acb", "abc", "adf", "abcd", "dbga", "addgd"]
    # words = ["baidu.com", "qq.com"]
    for word in words:
        trie.insert(word)

    queries = ["acb", "acbc", "adbf", "abcd", "dbga", "add"]
    # queries = ["baidu.com", "qq.com"]
    for query in queries:
        exists = trie.search(query)
        if exists:
            print("query: ", query, " exists")
        else:
            print("query: ", query, " not exists")
