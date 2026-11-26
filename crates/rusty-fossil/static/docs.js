"use strict";
const methods = ["get", "post", "put", "patch", "delete", "head", "options", "trace"];
function node(tag, className) {
    const result = document.createElement(tag);
    if (className)
        result.className = className;
    return result;
}
function codeBlock(label, value) {
    const section = node("section", "schema");
    const heading = node("h4");
    heading.textContent = label;
    const pre = node("pre");
    pre.textContent = typeof value === "string" ? value : JSON.stringify(value, null, 2) ?? "";
    section.append(heading, pre);
    return section;
}
function renderContent(parent, content) {
    if (!content)
        return;
    for (const [mediaType, media] of Object.entries(content)) {
        const details = node("details");
        const summary = node("summary");
        summary.textContent = mediaType;
        details.append(summary);
        if (media.schema !== undefined)
            details.append(codeBlock("Schema", media.schema));
        if (media.example !== undefined)
            details.append(codeBlock("Example", media.example));
        if (media.examples) {
            for (const [name, example] of Object.entries(media.examples)) {
                details.append(codeBlock(example.summary ?? `Example: ${name}`, example.value));
            }
        }
        parent.append(details);
    }
}
function renderOperation(path, method, operation) {
    const article = node("article", "route");
    const heading = node("div", "route-heading");
    const verb = node("span", "method");
    verb.textContent = method.toUpperCase();
    const route = node("code");
    route.textContent = path;
    const title = node("h2");
    title.textContent = operation.summary ?? operation.operationId ?? "API operation";
    heading.append(verb, route, title);
    article.append(heading);
    if (operation.description) {
        const description = node("p", "description");
        description.textContent = operation.description;
        article.append(description);
    }
    if (operation.parameters?.length) {
        const parameters = node("section");
        const title = node("h3");
        title.textContent = "Parameters";
        parameters.append(title);
        for (const parameter of operation.parameters) {
            const details = node("details");
            const summary = node("summary");
            summary.textContent = `${parameter.name} (${parameter.in})${parameter.required ? " — required" : ""}`;
            details.append(summary);
            if (parameter.description) {
                const description = node("p");
                description.textContent = parameter.description;
                details.append(description);
            }
            if (parameter.schema !== undefined)
                details.append(codeBlock("Schema", parameter.schema));
            parameters.append(details);
        }
        article.append(parameters);
    }
    if (operation.requestBody) {
        const body = node("section");
        const title = node("h3");
        title.textContent = `Request body${operation.requestBody.required ? " — required" : ""}`;
        body.append(title);
        if (operation.requestBody.description) {
            const description = node("p");
            description.textContent = operation.requestBody.description;
            body.append(description);
        }
        renderContent(body, operation.requestBody.content);
        article.append(body);
    }
    const responses = node("section");
    const responseHeading = node("h3");
    responseHeading.textContent = "Responses";
    responses.append(responseHeading);
    for (const [status, response] of Object.entries(operation.responses ?? {})) {
        const details = node("details");
        const summary = node("summary");
        summary.textContent = `${status} — ${response.description ?? "Response"}`;
        details.append(summary);
        if (response.headers && Object.keys(response.headers).length > 0) {
            const headers = node("section");
            const headersHeading = node("h4");
            headersHeading.textContent = "Headers";
            const list = node("ul");
            for (const [name, header] of Object.entries(response.headers)) {
                const item = node("li");
                item.textContent = `${name}${header.required ? " (required)" : ""}${header.description ? ` — ${header.description}` : ""}`;
                list.append(item);
                if (header.schema !== undefined)
                    details.append(codeBlock(`${name} schema`, header.schema));
                if (header.example !== undefined)
                    details.append(codeBlock(`${name} example`, header.example));
            }
            headers.append(headersHeading, list);
            details.append(headers);
        }
        renderContent(details, response.content);
        responses.append(details);
    }
    article.append(responses);
    return article;
}
function renderDocument(root, documentSpec) {
    root.replaceChildren();
    const heading = node("h2");
    heading.textContent = `${documentSpec.info.title} (${documentSpec.info.version})`;
    root.append(heading);
    if (documentSpec.info.description) {
        const description = node("p");
        description.textContent = documentSpec.info.description;
        root.append(description);
    }
    if (documentSpec.tags?.length) {
        const tags = node("section");
        const tagsHeading = node("h3");
        tagsHeading.textContent = "Endpoint groups";
        const list = node("ul");
        for (const tag of documentSpec.tags) {
            const item = node("li");
            item.textContent = tag.description ? `${tag.name}: ${tag.description}` : tag.name;
            list.append(item);
        }
        tags.append(tagsHeading, list);
        root.append(tags);
    }
    for (const [path, pathItem] of Object.entries(documentSpec.paths)) {
        for (const method of methods) {
            const operation = pathItem[method];
            if (operation)
                root.append(renderOperation(path, method, operation));
        }
    }
    const schemas = documentSpec.components?.schemas;
    if (schemas && Object.keys(schemas).length > 0) {
        const details = node("details");
        const summary = node("summary");
        summary.textContent = "Reusable schemas";
        details.append(summary);
        for (const [name, schema] of Object.entries(schemas)) {
            details.append(codeBlock(name, schema));
        }
        root.append(details);
    }
}
async function loadDocumentation() {
    const root = document.querySelector("#api-docs");
    if (!root)
        return;
    try {
        const response = await fetch("../openapi.json", { headers: { Accept: "application/json" } });
        if (!response.ok)
            throw new Error(`OpenAPI request failed with HTTP ${response.status}`);
        const documentSpec = (await response.json());
        renderDocument(root, documentSpec);
    }
    catch (error) {
        const message = node("p", "error");
        message.textContent = `Could not load the API documentation: ${error instanceof Error ? error.message : "unknown error"}`;
        root.replaceChildren(message);
    }
}
void loadDocumentation();
