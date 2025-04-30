package base

//lua script for pick score nearby element from sorted set
const LuaScriptOfPickSortedNearMember = `
-- KEYS[1]: sorted set 的 key
-- ARGV[1]: 目标 score

local zset_key = KEYS[1]
local target_score = tonumber(ARGV[1])

-- 查找比目标 score 严格大的最小一个元素
local result = redis.call('ZRANGEBYSCORE', zset_key, '(' .. target_score, '+inf', 'WITHSCORES', 'LIMIT', 0, 1)

if #result == 0 then
    return {}
end

local member = result[1]
local score = result[2]

-- 删除并返回 result
redis.call('ZREM', zset_key, result[1])

-- 返回 [member, score]
return {member, score}
`
