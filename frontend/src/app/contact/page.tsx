import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Github } from "lucide-react"
import Link from "next/link"

export default function ContactPage() {
  const developers = [
    { name: "Chaelim Heo", role: "Front-end development", github: "h0zzae" },
    { name: "Hanjeong Lee", role: "Microsoft Azure dataset collection", github: "leehanjeong" },
    { name: "Hyeonyoung Lee", role: "Google Cloud dataset collection", github: "wynter122" },
    { name: "Jaeil Hwang", role: "Server-side development", github: "chris0765" },
    { name: "Jungmyeong Park", role: "Front-end development", github: "j-myeong" },
    { name: "Kyunghwan Kim", role: "Database optimization", github: "red0sena" },
    { name: "Sungjae Lee", role: "AWS dataset collection", github: "james-sungjae-lee" },
  ]

  return (
    <div className="space-y-8">
      <Card>
        <CardHeader>
          <CardTitle className="text-2xl">Contact Information</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <p className="text-muted-foreground leading-relaxed">
            SpotLake system is maintained by Distributed Data Processing System
            Lab (DDPS Lab,{" "}
            <Link
              href="https://ddps.cloud"
              target="_blank"
              className="text-primary hover:underline"
            >
              https://ddps.cloud
            </Link>
            ) at Hanyang University.
          </p>
          <p className="text-muted-foreground leading-relaxed">
            If you have any question, suggestion, or request, you can contact
            email (
            <Link
              href="mailto:ddpslab@hanyang.ac.kr"
              className="text-primary hover:underline"
            >
              ddpslab@hanyang.ac.kr
            </Link>
            ) or create issue on GitHub repository
          </p>
          <div className="flex flex-wrap gap-4">
            <Button asChild variant="outline">
              <Link
                href="https://github.com/ddps-lab/spotlake"
                target="_blank"
              >
                <Github className="mr-2 h-4 w-4" />
                Github SpotLake
              </Link>
            </Button>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-2xl">Contributing Developers</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-muted-foreground mb-6">
            (names in the alphabetical order)
          </p>
          <ul className="space-y-4">
            {developers.map((dev) => (
              <li key={dev.github} className="flex flex-col sm:flex-row sm:items-center gap-2">
                <span className="font-medium">{dev.name}</span>: {dev.role}
                <Button asChild variant="ghost" size="sm" className="w-fit">
                  <Link
                    href={`https://github.com/${dev.github}`}
                    target="_blank"
                  >
                    <Github className="mr-2 h-4 w-4" />@{dev.github}
                  </Link>
                </Button>
              </li>
            ))}
          </ul>
        </CardContent>
      </Card>
    </div>
  )
}
