

export const utf8Encoder = new TextEncoder();


const isReserved = (key: string) => {
    return /^(?:do|if|in|for|let|new|try|var|case|else|enum|eval|false|null|this|true|void|with|break|catch|class|const|super|throw|while|yield|delete|export|import|public|return|static|switch|typeof|default|extends|finally|package|private|continue|debugger|function|arguments|interface|protected|implements|instanceof)$/.test(key);
};

const isDotProAccessor = (prop: string) => {
    return /^[a-zA-Z_$][0-9a-zA-Z_$]*$/.test(prop);
}

export const safePropAccessor = (prop: string) => {
    if (!isDotProAccessor(prop) || isReserved(prop)) {
        return `["${prop.replace(/\\/g, '\\\\').replace(/"/g, '\\\"')}"]`
    }
    return `.${prop}`;
}

export const safePropName = (prop: string) => {
    if (!isDotProAccessor(prop) || isReserved(prop)) {
        return `["${prop.replace(/\\/g, '\\\\').replace(/"/g, '\\\"')}"]`
    }
    return prop;
}


