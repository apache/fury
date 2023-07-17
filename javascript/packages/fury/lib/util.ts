

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


