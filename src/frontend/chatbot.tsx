'use client'

import { useState, useEffect } from 'react'
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Card, CardContent } from "@/components/ui/card"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Upload, Trash2, Send } from "lucide-react"

interface Document {
  id: string
  name: string
}

export default function RAGChatbot() {
  const [messages, setMessages] = useState<{ role: 'user' | 'bot', content: string }[]>([
    { role: 'bot', content: "Hello! I'm your AI assistant. How can I help you today?" }
  ])
  const [input, setInput] = useState('')
  const [documents, setDocuments] = useState<Document[]>([])

  useEffect(() => {
    fetchDocuments()
  }, [])

  const fetchDocuments = async () => {
    try {
      const response = await fetch('/api/documents')
      const data = await response.json()
      setDocuments(data)
    } catch (error) {
      console.error('Error fetching documents:', error)
    }
  }

  const handleSend = async () => {
    if (!input.trim()) return

    const newMessages = [...messages, { role: 'user', content: input }]
    setMessages(newMessages)
    setInput('')

    try {
      const response = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: input }),
      })
      const data = await response.json()
      setMessages([...newMessages, { role: 'bot', content: data.reply }])
    } catch (error) {
      console.error('Error sending message:', error)
    }
  }

  const handleFileUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (!file) return

    const formData = new FormData()
    formData.append('file', file)

    try {
      await fetch('/api/upload', {
        method: 'POST',
        body: formData,
      })
      fetchDocuments()
    } catch (error) {
      console.error('Error uploading file:', error)
    }
  }

  const handleFileDelete = async (documentId: string) => {
    try {
      await fetch(`/api/documents/${documentId}`, {
        method: 'DELETE',
      })
      fetchDocuments()
    } catch (error) {
      console.error('Error deleting file:', error)
    }
  }

  return (
    <div className="flex h-screen bg-gray-100">
      <div className="w-64 bg-white p-4 border-r">
        <h2 className="text-lg font-semibold mb-4">Uploaded Documents</h2>
        <ScrollArea className="h-[calc(100vh-8rem)]">
          {documents.map((doc) => (
            <div key={doc.id} className="flex justify-between items-center mb-2">
              <span className="truncate">{doc.name}</span>
              <Button variant="ghost" size="icon" onClick={() => handleFileDelete(doc.id)}>
                <Trash2 className="h-4 w-4" />
              </Button>
            </div>
          ))}
        </ScrollArea>
      </div>
      <div className="flex-1 flex flex-col overflow-hidden">
        <header className="bg-white border-b p-4">
          <div className="flex justify-between items-center">
            <h1 className="text-2xl font-bold">RAG Chatbot</h1>
            <div>
              <Input
                type="file"
                onChange={handleFileUpload}
                className="hidden"
                id="file-upload"
              />
              <label htmlFor="file-upload" className="cursor-pointer">
                <Button variant="outline">
                  <Upload className="h-4 w-4 mr-2" />
                  Upload File
                </Button>
              </label>
            </div>
          </div>
        </header>
        <main className="flex-1 overflow-hidden">
          <Card className="h-full flex flex-col">
            <CardContent className="flex-1 overflow-hidden flex flex-col p-4">
              <ScrollArea className="flex-1 pr-4">
                {messages.map((message, index) => (
                  <div key={index} className={`mb-4 ${message.role === 'user' ? 'text-right' : 'text-left'}`}>
                    <span className={`inline-block p-2 rounded-lg ${message.role === 'user' ? 'bg-blue-500 text-white' : 'bg-gray-200 text-gray-800'}`}>
                      {message.content}
                    </span>
                  </div>
                ))}
              </ScrollArea>
              <div className="pt-4 border-t">
                <div className="flex space-x-2">
                  <Input
                    type="text"
                    placeholder="Type your message..."
                    value={input}
                    onChange={(e) => setInput(e.target.value)}
                    onKeyPress={(e) => e.key === 'Enter' && handleSend()}
                  />
                  <Button onClick={handleSend}>
                    <Send className="h-4 w-4" />
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>
        </main>
      </div>
    </div>
  )
}