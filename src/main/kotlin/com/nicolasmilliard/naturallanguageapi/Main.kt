package com.nicolasmilliard.naturallanguageapi

import com.google.api.gax.rpc.InvalidArgumentException
import com.google.cloud.language.v1.*
import java.io.File
import java.time.LocalDateTime
import kotlin.system.exitProcess
import okio.*


fun main(args: Array<String>) {
    println("Natural language API")
    println("Using service account file: ${System.getenv("GOOGLE_APPLICATION_CREDENTIALS")}")
    println("Input file: ${args[0]}")

    var inputFile = File(
        args[0]
            .replaceFirst("~", System.getProperty("user.home"))
    )

    if (!inputFile.exists()) {
        System.err.println("Input file does not exist.")
        exitProcess(1)
    }

    val now = LocalDateTime.now()

    val outputFile = File("results_$now.csv")
    val errorFile = File("errors_$now.csv")

    val client = LanguageServiceClient.create()
    val nlpClassifier = NLPClassifier(
        client, 10,
        SentimentProcessor() //,
//        ClassifierProcessor()
    )

    println("---------------------------")
    nlpClassifier.analyzeSentimentAndClassifyText(inputFile, outputFile, errorFile)

}

class NLPClassifier(
    private val client: LanguageServiceClient,
    private val quota: Int,
    vararg val processors: NlpProcessor
) {

    fun analyzeSentimentAndClassifyText(inputFile: File, outputFile: File, errorFile: File) {

        client.use { client ->
            inputFile.source().use { inputSource ->
                inputSource.buffer().use { inputBufferSource ->
                    outputFile.sink().use { outputSink ->
                        outputSink.buffer().use { outputBufferSink ->
                            errorFile.sink().use { errorsSink ->
                                errorsSink.buffer().use { errorsBufferSink ->
                                    internal(inputBufferSource, outputBufferSink, errorsBufferSink)
                                }
                            }
                        }
                    }
                }

            }
        }
    }

    private fun internal(inputSource: BufferedSource, outputSink: BufferedSink, errorsSink: BufferedSink) {
        var remaining = quota
        var proccessCount = 0
        var errorsCount = 0

        // Write fields description
        println("Fields description")
        processors.forEach {
            println(it.fieldDescriptions)
        }

        // Write CSV Headers
        outputSink.writeUtf8("Input")
        processors.forEach {
            outputSink
                .writeUtf8(",")
                .writeUtf8(it.fieldNames)
        }
        outputSink.writeUtf8("\n")
        errorsSink.writeUtf8("Input, Status Code, Error message\n")

        // Process each input lines
        while (remaining > 0) {
            --remaining
            val line = inputSource.readUtf8Line() ?: break

            val builder = StringBuilder()
            try {
                processors.forEach {
                    val formatted = it.process(client, line)
                    builder
                        .append(",")
                        .append(formatted)
                }

                // Write results
                outputSink
                    .writeUtf8(line)
                    .writeUtf8(builder.toString())
                    .writeUtf8("\n")
                ++proccessCount
            } catch (e: InvalidArgumentException) {
                builder.append(",")
                    .append(e.statusCode)
                    .append(",")
                    .append(e.toString())
                // Write errors
                errorsSink.writeUtf8(line)
                    .writeUtf8(builder.toString())
                    .writeUtf8("\n")
                ++errorsCount
            }
        }
        println("---------------------------")
        println("$proccessCount input successfully processed")
        println("$errorsCount errors encountered")
    }
}

interface NlpProcessor {
    val fieldDescriptions: String
    val fieldNames: String
    fun process(client: LanguageServiceClient, text: String): String
}

class SentimentProcessor : NlpProcessor {
    override val fieldNames: String
        get() = "Sentiment Score,Sentiment Magnitude"
    override val fieldDescriptions: String
        get() = """
            Sentiment Score -> "score of the sentiment ranges between -1.0 (negative) and 1.0 (positive) and corresponds to the overall emotional leaning of the text.",
            Sentiment Magnitude -> "magnitude indicates the overall strength of emotion (both positive and negative) within the given text, between 0.0 and +inf. Unlike score, magnitude is not normalized; each expression of emotion within the text (both positive and negative) contributes to the text's magnitude (so longer text blocks may have greater magnitudes)."
        """.trimIndent()


    override fun process(client: LanguageServiceClient, text: String): String {
        return format(analyzeSentiment(client, text))
    }

    private fun analyzeSentiment(client: LanguageServiceClient, text: String): Sentiment {
        val doc = Document.newBuilder()
            .setContent(text)
            .setType(Document.Type.PLAIN_TEXT)
            .build()

        return client.analyzeSentiment(doc).documentSentiment
    }

    private fun format(sentiment: Sentiment): String {
        return "${sentiment.score},${sentiment.magnitude}"
    }
}

class ClassifierProcessor : NlpProcessor {
    override val fieldNames: String
        get() = "Categories"
    override val fieldDescriptions: String
        get() = """
            Category confidence -> "The classifier's confidence of the category. Number represents how certain the classifier is that this category represents the given text."
        """.trimIndent()


    override fun process(client: LanguageServiceClient, text: String): String {
        return format(classifierApi(client, text))
    }

    private fun classifierApi(client: LanguageServiceClient, text: String): List<ClassificationCategory> {
        val doc = Document.newBuilder()
            .setContent(text)
            .setType(Document.Type.PLAIN_TEXT)
            .build()


        val request = ClassifyTextRequest.newBuilder()
            .setDocument(doc)
            .build()

        return client.classifyText(request).categoriesList
    }

    private fun format(categories: List<ClassificationCategory>): String {
        val builder = StringBuilder()
        categories.forEach {
            builder.append(",")
            builder.append(it.name)
        }
        categories.forEach {
            builder.append(",")
            builder.append(it.confidence)
        }
        return builder.toString()
    }
}


