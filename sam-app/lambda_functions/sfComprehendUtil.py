"""
You must have an AWS account to use the Amazon Connect CTI Adapter.
Downloading and/or using the Amazon Connect CTI Adapter is subject to the terms of the AWS Customer Agreement,
AWS Service Terms, and AWS Privacy Notice.

© 2017, Amazon Web Services, Inc. or its affiliates. All rights reserved.

NOTE:  Other license terms may apply to certain, identified software components
contained within or distributed with the Amazon Connect CTI Adapter if such terms are
included in the LibPhoneNumber-js and Salesforce Open CTI. For such identified components,
such other license terms will then apply in lieu of the terms above.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import logging
import boto3
import json

logger = logging.getLogger()


def StartComprehendAnalysis(transcripts, compAnalysis, languageCode, segments):
    if segments:
        logger.info("Analyzing Invdividual Segmented Detail...")
        cResults = analyzeContactSegments(transcripts, compAnalysis, languageCode)
    else:
        logger.info("Analyzing Entire Transcript Detail...")
        cResults = analyzeContactDetail(transcripts, compAnalysis, languageCode)

    logger.info("Comprehend Analysis: {}".format(cResults))
    return cResults

def analyzeContactSegments(transcripts, compAnalysis, languageCode):
    
    comprehend = boto3.client(service_name='comprehend')
    rComprehend = {}

    for transcript in transcripts:
        if compAnalysis == "snt":
            rComprehend = detectSentiment(comprehend, transcript['content'], languageCode)
        elif compAnalysis == "kw":
            rComprehend = detectKeyWords(comprehend, transcript['content'], languageCode)
        elif compAnalysis == "dl":
            rComprehend = detectDominantLanguage(comprehend, transcript['content'])
        elif compAnalysis == "ne": 
            rComprehend = detectNamedEntities(comprehend, transcript['content'], languageCode)
        elif compAnalysis == "syn":
            rComprehend = detectSyntax(comprehend, transcript['content'], languageCode)
      
    return rComprehend

def analyzeContactDetail(transcripts, compAnalysis, languageCode):
    comprehend = boto3.client(service_name='comprehend')
    rComprehend = {}

    #concat segments
    finalTranscript = ''
    for transcript in transcripts:
        finalTranscript += transcript['content']+' '

    if compAnalysis == "snt":
      rComprehend = detectSentiment(comprehend, finalTranscript, languageCode)
    elif compAnalysis == "kw":
      rComprehend = detectKeyWords(comprehend, finalTranscript, languageCode)
    elif compAnalysis == "dl":
      rComprehend = detectDominantLanguage(comprehend, finalTranscript)
    elif compAnalysis == "ne":
      rComprehend = detectNamedEntities(comprehend, finalTranscript, languageCode)
    elif compAnalysis == "syn":
      rComprehend = detectSyntax(comprehend, finalTranscript, languageCode)

    return rComprehend

def detectSentiment(cObject, sentimentText, languageCode):
  logger.info('Detecting Sentiment for: %s' % sentimentText)
  rSNT = cObject.detect_sentiment(Text=sentimentText, LanguageCode=languageCode)
  return rSNT

def detectKeyWords(cObject, keyWordText, languageCode):
  logger.info('Detecting Key Phrases for: %s' % keyWordText)
  rKW = cObject.detect_key_phrases(Text=keyWordText, LanguageCode=languageCode)
  return rKW

def detectDominantLanguage(cObject, languageText):
  logger.info('Detecting Dominant Language for: %s' % languageText)
  rDL = cObject.detect_dominant_language(Text = languageText)
  return rDL

def detectNamedEntities(cObject, entityText, languageCode):
  logger.info('Detecting Named Entities for: %s' % entityText)
  rNE = cObject.detect_entities(Text=entityText, LanguageCode=languageCode)
  return rNE

def detectSyntax(cObject, syntaxText, languageCode):
  logger.info('Detecting Syntax for: %s' % syntaxText)
  rDS = cObject.detect_syntax(Text=syntaxText, LanguageCode=languageCode)
  return rDS 

def GetFormattedSentiment(data):
    score = 0.0

    if data['Sentiment'] == 'POSITIVE':
        score = data['SentimentScore']['Positive']
    elif data['Sentiment'] == 'NEGATIVE':
        score = data['SentimentScore']['Negative']
    elif data['Sentiment'] == 'NEUTRAL':
        score = data['SentimentScore']['Neutral']
    elif data['Sentiment'] == 'MIXED':
        score = data['SentimentScore']['Mixed']

    return data['Sentiment'] + ', ' + str(score)

def GetFormattedKeywords(data):
    keywords = []
    for phrase in data['KeyPhrases']:
        if len(', '.join(keywords)) + len(phrase['Text']) > 131000:
            break
        keywords.append(phrase['Text'])
    
    return ', '.join(keywords)

def GetFormattedDominantLanguage(data):
    return data['Languages'][0]['LanguageCode']

def GetFormattedNamedEntities(data):
    namedEntities = []
    for entity in data['Entities']:
        if len(', '.join(namedEntities)) + len(entity['Text']+':'+entity['Type']) > 131000:
            break
        namedEntities.append(entity['Text']+':'+entity['Type'])
    return ', '.join(namedEntities)

def GetFormattedSyntax(data):
    dMap = dict()
    entries = []
    value = 1

    for syntaxToken in data['SyntaxTokens']:
        key = syntaxToken['PartOfSpeech']['Tag']+':'+syntaxToken['Text']
        if key in dMap:
            value = dMap[key]
            value = value + 1
        else:
            value = 1
        dMap[key] = value

    for item in dMap:
        entry = {'text':item, 'size':dMap[item]}
        entries.append(entry)

    return json.dumps(entries)

def processTranscript(iItems):
    transcripts = []
    for iTranscript in iItems:
        transcript = {}
        if 'start_time' not in iTranscript:
            if iTranscript['type'] == 'punctuation':
                if len(transcripts) > 0:
                    lastItem = transcripts[len(transcripts)-1]
                    lastChar = lastItem['content'][len(lastItem['content'])-1]
                    if(lastChar != '.' and lastChar != ',' and lastChar != '?' and lastChar != ':' and lastChar != '!'):
                        lastItem['content'] += iTranscript['alternatives'][0]['content']
                        continue
            continue
        transcript['start_time'] = float(iTranscript['start_time'])
        transcript['end_time'] = float(iTranscript['end_time'])
        maxAlternativeConfidenceScore = 0.0
        selectedAlternative = ''
        for alternative in iTranscript['alternatives']:
            if(float(alternative['confidence']) > maxAlternativeConfidenceScore):
                selectedAlternative = alternative['content']
        transcript['content'] = selectedAlternative
        if(len(transcripts)>0):
            lastItem = transcripts[len(transcripts)-1]
            lastChar = lastItem['content'][len(lastItem['content'])-1]
            if (float(transcript['start_time']) - float(lastItem['start_time']) <= 2.0) and (lastChar != '.' and lastChar != ',' and lastChar != '?' and lastChar != ':' and lastChar != '!'):
               lastItem['content'] += ' '+ selectedAlternative
            else:
                transcripts.append(transcript)
        else:
            transcripts.append(transcript)
    return transcripts

    
        

