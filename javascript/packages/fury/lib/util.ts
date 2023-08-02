/*
 * Copyright 2023 The Fury Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

export const utf8Encoder = new TextEncoder();


const isReserved = (key: string) => {
    return /^(?:do|if|in|for|let|new|try|var|case|else|enum|eval|false|null|this|true|void|with|break|catch|class|const|super|throw|while|yield|delete|export|import|public|return|static|switch|typeof|default|extends|finally|package|private|continue|debugger|function|arguments|interface|protected|implements|instanceof)$/.test(key);
};

const isDotPropAccessor = (prop: string) => {
    return /^[a-zA-Z_$][0-9a-zA-Z_$]*$/.test(prop);
}


export const replaceBackslashAndQuote = (v: string) => {
    return v.replace(/\\/g, '\\\\').replace(/"/g, '\\"')
}

export const safePropAccessor = (prop: string) => {
    if (!isDotPropAccessor(prop) || isReserved(prop)) {
        return `["${replaceBackslashAndQuote(prop)}"]`
    }
    return `.${prop}`;
}

export const safePropName = (prop: string) => {
    if (!isDotPropAccessor(prop) || isReserved(prop)) {
        return `["${replaceBackslashAndQuote(prop)}"]`
    }
    return prop;
}

export const isNodeEnv = typeof process === "object" && process.env.ECMA_ONLY === 'true' && typeof require === "function";