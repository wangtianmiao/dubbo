/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.dubbo.rpc.cluster.router.condition;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.common.utils.UrlUtils;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.cluster.Router;
import com.alibaba.dubbo.rpc.cluster.router.AbstractRouter;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ConditionRouter
 */
public class ConditionRouter extends AbstractRouter {

    private static final Logger logger = LoggerFactory.getLogger(ConditionRouter.class);
    private static final int DEFAULT_PRIORITY = 2;
    private static Pattern ROUTE_PATTERN = Pattern.compile("([&!=,]*)\\s*([^&!=,\\s]+)");
    private final boolean force;
    private final Map<String, MatchPair> whenCondition;
    private final Map<String, MatchPair> thenCondition;

    public ConditionRouter(URL url) {
        this.url = url;
        this.priority = url.getParameter(Constants.PRIORITY_KEY, DEFAULT_PRIORITY);
        this.force = url.getParameter(Constants.FORCE_KEY, false);
        try {
            String rule = url.getParameterAndDecoded(Constants.RULE_KEY);   // 获取路由规则
            if (rule == null || rule.trim().length() == 0) {
                throw new IllegalArgumentException("Illegal route rule!");
            }
            rule = rule.replace("consumer.", "").replace("provider.", "");
            int i = rule.indexOf("=>"); // 定位 => 分隔符
            String whenRule = i < 0 ? null : rule.substring(0, i).trim();   // 获取服务消费者匹配规则
            String thenRule = i < 0 ? rule.trim() : rule.substring(i + 2).trim();   // 获取提供者匹配规则
            Map<String, MatchPair> when = StringUtils.isBlank(whenRule) || "true".equals(whenRule) ? new HashMap<String, MatchPair>() : parseRule(whenRule);    // 解析服务消费者匹配规则
            Map<String, MatchPair> then = StringUtils.isBlank(thenRule) || "false".equals(thenRule) ? null : parseRule(thenRule);   // 解析服务提供者匹配规则
            // NOTE: It should be determined on the business level whether the `When condition` can be empty or not.
            this.whenCondition = when;  // 将解析出的匹配规则赋值给 whenCondition 成员变量
            this.thenCondition = then;  // 将解析出的匹配规则赋值给 thenCondition 成员变量
        } catch (ParseException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private static Map<String, MatchPair> parseRule(String rule)
            throws ParseException {
        Map<String, MatchPair> condition = new HashMap<String, MatchPair>();    // 定义条件映射集合
        if (StringUtils.isBlank(rule)) {
            return condition;
        }
        // Key-Value pair, stores both match and mismatch conditions
        MatchPair pair = null;
        // Multiple values
        Set<String> values = null;
        final Matcher matcher = ROUTE_PATTERN.matcher(rule);
        while (matcher.find()) { // Try to match one by one
            String separator = matcher.group(1);    // 获取括号一内的匹配结果
            String content = matcher.group(2);      // 获取括号二内的匹配结果
            // Start part of the condition expression.
            if (separator == null || separator.length() == 0) { // 分隔符为空，表示匹配的是表达式的开始部分
                pair = new MatchPair(); // 创建 MatchPair 对象
                condition.put(content, pair);   // 存储 <匹配项, MatchPair> 键值对，比如 <host, MatchPair>
            }
            // The KV part of the condition expression
            else if ("&".equals(separator)) {   // 如果分隔符为 &，表明接下来也是一个条件
                if (condition.get(content) == null) {   // 尝试从 condition 获取 MatchPair
                    pair = new MatchPair(); // 未获取到 MatchPair，重新创建一个，并放入 condition 中
                    condition.put(content, pair);
                } else {
                    pair = condition.get(content);
                }
            }
            // The Value in the KV part.
            else if ("=".equals(separator)) {   // 分隔符为 =
                if (pair == null)
                    throw new ParseException("Illegal route rule \""
                            + rule + "\", The error char '" + separator
                            + "' at index " + matcher.start() + " before \""
                            + content + "\".", matcher.start());

                values = pair.matches;
                values.add(content);    // 将 content 存入到 MatchPair 的 matches 集合中
            }
            // The Value in the KV part.
            else if ("!=".equals(separator)) {  //  分隔符为 !=
                if (pair == null)
                    throw new ParseException("Illegal route rule \""
                            + rule + "\", The error char '" + separator
                            + "' at index " + matcher.start() + " before \""
                            + content + "\".", matcher.start());

                values = pair.mismatches;
                values.add(content);    // 将 content 存入到 MatchPair 的 mismatches 集合中
            }
            // The Value in the KV part, if Value have more than one items.
            else if (",".equals(separator)) { // Should be seperateed by ','    // 分隔符为 ,
                if (values == null || values.isEmpty())
                    throw new ParseException("Illegal route rule \""
                            + rule + "\", The error char '" + separator
                            + "' at index " + matcher.start() + " before \""
                            + content + "\".", matcher.start());
                values.add(content);    // 将 content 存入到上一步获取到的 values 中，可能是 matches，也可能是 mismatches
            } else {
                throw new ParseException("Illegal route rule \"" + rule
                        + "\", The error char '" + separator + "' at index "
                        + matcher.start() + " before \"" + content + "\".", matcher.start());
            }
        }
        return condition;
    }

    @Override
    public <T> List<Invoker<T>> route(List<Invoker<T>> invokers, URL url, Invocation invocation)
            throws RpcException {
        if (invokers == null || invokers.isEmpty()) {
            return invokers;
        }
        try {
            if (!matchWhen(url, invocation)) {  // 先对服务消费者条件进行匹配，如果匹配失败，表明服务消费者 url 不符合匹配规则，无需进行后续匹配，直接返回 Invoker 列表即可。
                return invokers;
            }
            List<Invoker<T>> result = new ArrayList<Invoker<T>>();
            if (thenCondition == null) {    // 服务提供者匹配条件未配置，表明对指定的服务消费者禁用服务，也就是服务消费者在黑名单中
                logger.warn("The current consumer in the service blacklist. consumer: " + NetUtils.getLocalHost() + ", service: " + url.getServiceKey());
                return result;
            }
            for (Invoker<T> invoker : invokers) {   // 这里可以简单的把 Invoker 理解为服务提供者，现在使用服务提供者匹配规则对 Invoker 列表进行匹配
                if (matchThen(invoker.getUrl(), url)) { // 若匹配成功，表明当前 Invoker 符合服务提供者匹配规则。
                    result.add(invoker);    // 此时将 Invoker 添加到 result 列表中
                }
            }
            if (!result.isEmpty()) {    // 返回匹配结果，如果 result 为空列表，且 force = true，表示强制返回空列表，否则路由结果为空的路由规则将自动失效
                return result;
            } else if (force) {
                logger.warn("The route result is empty and force execute. consumer: " + NetUtils.getLocalHost() + ", service: " + url.getServiceKey() + ", router: " + url.getParameterAndDecoded(Constants.RULE_KEY));
                return result;
            }
        } catch (Throwable t) {
            logger.error("Failed to execute condition router rule: " + getUrl() + ", invokers: " + invokers + ", cause: " + t.getMessage(), t);
        }
        return invokers;    // 原样返回，此时 force = false，表示该条路由规则失效
    }

    @Override
    public URL getUrl() {
        return url;
    }

    @Override
    public int compareTo(Router o) {
        if (o == null || o.getClass() != ConditionRouter.class) {
            return 1;
        }
        ConditionRouter c = (ConditionRouter) o;
        return this.priority == c.priority ? url.toFullString().compareTo(c.url.toFullString()) : (this.priority > c.priority ? 1 : -1);
    }

    boolean matchWhen(URL url, Invocation invocation) {
        return whenCondition == null || whenCondition.isEmpty() || matchCondition(whenCondition, url, null, invocation);    // 服务消费者条件为 null 或空，均返回 true
    }

    private boolean matchThen(URL url, URL param) {
        return !(thenCondition == null || thenCondition.isEmpty()) && matchCondition(thenCondition, url, param, null);  // 服务提供者条件为 null 或空，表示禁用服务
    }

    private boolean matchCondition(Map<String, MatchPair> condition, URL url, URL param, Invocation invocation) {
        Map<String, String> sample = url.toMap();   // 将服务提供者或消费者 url 转成 Map
        boolean result = false;
        for (Map.Entry<String, MatchPair> matchPair : condition.entrySet()) {   // 遍历 condition 列表
            String key = matchPair.getKey();    // 获取匹配项名称，比如 host、method 等
            String sampleValue;
            //get real invoked method name from invocation
            if (invocation != null && (Constants.METHOD_KEY.equals(key) || Constants.METHODS_KEY.equals(key))) {    // 如果 invocation 不为空，且 key 为 method(s)，表示进行方法匹配
                sampleValue = invocation.getMethodName();   // 从 invocation 获取被调用方法的名称
            } else {
                sampleValue = sample.get(key);  // 从服务提供者或消费者 url 中获取指定字段值，比如 host、application 等
                if (sampleValue == null) {
                    sampleValue = sample.get(Constants.DEFAULT_KEY_PREFIX + key);   // 尝试通过 default.xxx 获取相应的值
                }
            }
            if (sampleValue != null) {
                if (!matchPair.getValue().isMatch(sampleValue, param)) {    // 调用 MatchPair 的 isMatch 方法进行匹配
                    return false;   // 只要有一个规则匹配失败，立即返回 false 结束方法逻辑
                } else {
                    result = true;
                }
            } else {
                //not pass the condition
                if (!matchPair.getValue().matches.isEmpty()) {  // sampleValue 为空，表明服务提供者或消费者 url 中不包含相关字段。此时如果 MatchPair 的 matches 不为空，表示匹配失败，返回 false。
                    return false;
                } else {
                    result = true;
                }
            }
        }
        return result;
    }

    private static final class MatchPair {
        final Set<String> matches = new HashSet<String>();
        final Set<String> mismatches = new HashSet<String>();

        private boolean isMatch(String value, URL param) {
            if (!matches.isEmpty() && mismatches.isEmpty()) {   // 情况一：matches 非空，mismatches 为空
                for (String match : matches) {  // 遍历 matches 集合，检测入参 value 是否能被 matches 集合元素匹配到。
                    if (UrlUtils.isMatchGlobPattern(match, value, param)) {
                        return true;
                    }
                }
                return false;   // 如果所有匹配项都无法匹配到入参，则返回 false
            }

            if (!mismatches.isEmpty() && matches.isEmpty()) {   // 情况二：matches 为空，mismatches 非空
                for (String mismatch : mismatches) {
                    if (UrlUtils.isMatchGlobPattern(mismatch, value, param)) {  // 只要入参被 mismatches 集合中的任意一个元素匹配到，就返回 false
                        return false;
                    }
                }
                return true;
            }

            if (!matches.isEmpty() && !mismatches.isEmpty()) {  // 情况三：matches 非空，mismatches 非空
                //when both mismatches and matches contain the same value, then using mismatches first
                for (String mismatch : mismatches) {    // matches 和 mismatches 均为非空，此时优先使用 mismatches 集合元素对入参进行匹配。
                    if (UrlUtils.isMatchGlobPattern(mismatch, value, param)) {  // 只要 mismatches 集合中任意一个元素与入参匹配成功，就立即返回 false，结束方法逻辑
                        return false;
                    }
                }
                for (String match : matches) {  // mismatches 集合元素无法匹配到入参，此时再使用 matches 继续匹配
                    if (UrlUtils.isMatchGlobPattern(match, value, param)) { // 只要 matches 集合中任意一个元素与入参匹配成功，就立即返回 true
                        return true;
                    }
                }
                return false;   // 全部失配，则返回 false
            }
            return false;   // 情况四：matches 和 mismatches 均为空，此时返回 false
        }
    }
}
