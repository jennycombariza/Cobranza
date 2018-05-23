library(telegram)

bot <- telegram::TGBot$new(token = "329942951:AAFYggVtc2tuw-GVRxi3m6l8h49inDQsRfo")
bot$set_default_chat_id(325135785)
  
  messageTelegram <- function(bot, message){
    tryCatch(bot$sendMessage(message, parse = "markdown"),
             error = function(c) "Error telegramR didn't work",
             warning = function(c) "warning telegramR didn't work",
             message = function(c) "message telegramR didn't work")
  }
  
  messageElapsedTime <- function(bot, timer){
    message1 <- paste("Process finished ...")
    message2 <- paste("...", format(timer$toc - timer$tic, trim = 2), "seconds elapsed.")
    messageTelegram(bot, message1)
    messageTelegram(bot, message2)
  }
  
  messagePerformancePlot <- function(bot, plotName, caption){
    bot$sendPhoto(plotName, caption = caption)
  }
  