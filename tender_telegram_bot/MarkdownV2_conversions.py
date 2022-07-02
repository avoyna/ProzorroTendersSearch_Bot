def add_telegram_text_escaped_characters(telegram_message):
    text_escaped_characters = telegram_message.translate( telegram_message.maketrans({'_': '\_',
                                             '*': '\*',
                                             '[': '\[',
                                             ']': '\]',
                                             '(': '\(',
                                             ')': '\)',
                                             '~': '\~',
                                             "`": "\`",
                                             '>': '\>',
                                             '#': '\#',
                                             '+': '\+',
                                             '-': '\-',
                                             '=': '\=',
                                             '|': '\|',
                                             '{': '\{',
                                             '}': '\}',
                                             '.': "\.",
                                             '!': '\!',
                                             '\\': '\\\\'}))

    #print(text_escaped_characters)

    return(text_escaped_characters)


def add_telegram_link_escaped_characters(telegram_message):
    link_escaped_characters = telegram_message.translate(telegram_message.maketrans({'(': '\(',
                                             ')': '\)',
                                             '\\': '\\\\'}))
    #print(link_escaped_characters)

    return(link_escaped_characters)


def format_tech_message_markdownV2(err_module, err_message):
    result_str = "*" + add_telegram_text_escaped_characters(err_module) + "* \n\n_" + \
        add_telegram_text_escaped_characters(err_message) + "_"
    print(result_str)
    return result_str
