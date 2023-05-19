import heapq
push_q = []
heapq.heapify(push_q)

heapq.heappush(push_q, 1)
heapq.heappush(push_q, 1)
heapq.heappush(push_q, 1)
heapq.heappush(push_q, 2)
heapq.heappush(push_q, 1)

while 1 in push_q:
    push_q.remove(1)

print(push_q)
