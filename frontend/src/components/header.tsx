"use client"

import Link from "next/link"
import Image from "next/image"
import { Button } from "@/components/ui/button"
import { Github, Menu } from "lucide-react"
import { ModeToggle } from "@/components/mode-toggle"
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "@/components/ui/sheet"
import { useTheme } from "next-themes"
import { useEffect, useState } from "react"

// 과거 데이터 요청 폼. Terms of Use 와 동의 항목이 폼 문항에 들어 있어
// 사이트에서 따로 동의를 받지 않는다. 이 사이트를 찾는 주된 이유라
// 어느 페이지에서나 보이도록 헤더에 둔다.
const DATA_REQUEST_FORM = "https://forms.gle/GjhsuybJkUt5LMFc7"

export function Header() {
  const { resolvedTheme } = useTheme()
  const [mounted, setMounted] = useState(false)

  useEffect(() => {
    setMounted(true)
  }, [])

  const logoSrc = mounted && resolvedTheme === 'dark' 
    ? "/images/logo_ddps_dark.svg" 
    : "/images/logo_ddps_light.svg"

  return (
    <header className="sticky top-0 z-50 w-full border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60 flex justify-center">
      <div className="w-full max-w-7xl px-4 flex h-14 items-center">
        <div className="mr-4 flex">
          <Link href="/" className="mr-6 flex items-center space-x-2">
            <Image src={logoSrc} alt="DDPS" width={0} height={0} className="h-8 w-auto" style={{width: 'auto', height: '1.5rem'}} />
            <span className="text-gray-400 dark:text-gray-600 font-light">|</span>
            <span className="font-bold md:text-2xl sm:inline-block bg-clip-text text-transparent bg-gradient-to-r from-blue-600 to-cyan-500">
              SpotLake
            </span>
          </Link>
          <nav className="hidden md:flex items-center space-x-6 text-sm font-medium">
            <Link
              href="/"
              className="transition-colors hover:text-foreground/80 text-foreground/60"
            >
              Home
            </Link>
            <Link
              href="/about"
              className="transition-colors hover:text-foreground/80 text-foreground/60"
            >
              About
            </Link>
            <Link
              href="/document"
              className="transition-colors hover:text-foreground/80 text-foreground/60"
            >
              Document
            </Link>
          </nav>
        </div>
        <div className="flex flex-1 items-center justify-end space-x-2">
          <nav className="flex items-center space-x-2">
            <Button asChild size="sm" className="hidden sm:inline-flex">
              <Link href={DATA_REQUEST_FORM} target="_blank" rel="noreferrer">
                Request Full Dataset
              </Link>
            </Button>
            <Link
              href="https://github.com/ddps-lab/spotlake"
              target="_blank"
              rel="noreferrer"
            >
              <div
                className="inline-flex items-center justify-center whitespace-nowrap rounded-md text-sm font-medium ring-offset-background transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 hover:bg-accent hover:text-accent-foreground h-9 py-2 w-9 px-0"
              >
                <Github className="h-4 w-4" />
                <span className="sr-only">GitHub</span>
              </div>
            </Link>
            <ModeToggle />
            
            {/* Mobile Menu */}
            <Sheet>
              <SheetTrigger asChild>
                <Button variant="ghost" size="icon" className="md:hidden">
                  <Menu className="h-5 w-5" />
                  <span className="sr-only">Toggle menu</span>
                </Button>
              </SheetTrigger>
              <SheetContent side="right" className="w-[240px]">
                <SheetHeader className="text-left mb-4">
                  <SheetTitle>Menu</SheetTitle>
                </SheetHeader>
                <nav className="flex flex-col space-y-3">
                  <Link
                    href="/"
                    className="px-2 py-2 text-base font-medium transition-colors hover:text-foreground text-foreground/80 hover:bg-accent rounded-md"
                  >
                    Home
                  </Link>
                  <Link
                    href="/about"
                    className="px-2 py-2 text-base font-medium transition-colors hover:text-foreground text-foreground/80 hover:bg-accent rounded-md"
                  >
                    About
                  </Link>
                  <Link
                    href="/document"
                    className="px-2 py-2 text-base font-medium transition-colors hover:text-foreground text-foreground/80 hover:bg-accent rounded-md"
                  >
                    Document
                  </Link>
                  <Button asChild size="sm" className="mt-2 w-full">
                    <Link href={DATA_REQUEST_FORM} target="_blank" rel="noreferrer">
                      Request Full Dataset
                    </Link>
                  </Button>
                </nav>
              </SheetContent>
            </Sheet>
          </nav>
        </div>
      </div>
    </header>
  )
}
