import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Github, FileText } from "lucide-react"
import Link from "next/link"

export default function AboutPage() {
  return (
    <div className="space-y-8">
      <Card>
        <CardHeader>
          <CardTitle className="text-2xl">What is SpotLake system?</CardTitle>
        </CardHeader>
        <CardContent className="text-muted-foreground leading-relaxed">
          SpotLake system is an integrated data archive service that provides spot
          instance datasets collected from diverse public cloud vendors. The
          datasets include various information about spot instances like spot
          availability, spot interruption frequency, and spot price. Researchers
          and developers can utilize the SpotLake system to make their own system
          more cost-efficiently. SpotLake system currently provides the latest and
          restricted range of spot datasets collected from AWS, Google Cloud, and
          Azure through a demo page. We believe numerous systems could achieve a
          huge improvement in cost efficiency by utilizing the SpotLake system.
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-2xl">Paper and code</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <p className="text-muted-foreground leading-relaxed">
            If you are interested in an analysis of the SpotLake datasets or
            system implementation, check the latest version of the SpotLake paper
            which is published in IISWC 2022. We also published an older version
            of the paper through arXiv.
          </p>
          <div className="flex flex-wrap gap-4">
            <Button asChild variant="default">
              <Link
                href="https://ieeexplore.ieee.org/document/9975369"
                target="_blank"
              >
                <FileText className="mr-2 h-4 w-4" />
                IISWC 2022 paper
              </Link>
            </Button>
            <Button asChild variant="outline">
              <Link
                href="http://leeky.me/publications/spotlake.pdf"
                target="_blank"
              >
                <FileText className="mr-2 h-4 w-4" />
                PDF paper
              </Link>
            </Button>
          </div>
          <p className="text-muted-foreground leading-relaxed pt-4">
            Every source code and the issue of the SpotLake system is maintained
            through the GitHub repository. Anyone interested in the SpotLake
            system could contribute to the code. You can check the star button if
            you are intriguing this open-source project.
          </p>
          <div className="flex flex-wrap gap-4">
            <Button asChild variant="outline">
              <Link
                href="https://github.com/ddps-lab/spotlake"
                target="_blank"
              >
                <Github className="mr-2 h-4 w-4" />
                Github
              </Link>
            </Button>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-2xl">How to access full dataset</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <p className="text-muted-foreground leading-relaxed">
            We can not provide the full dataset through this web-service because
            the dataset is too large. Those who want to access the full dataset of
            the SpotLake system, please fill out the google form below and we will
            give you access permission for the full dataset.
          </p>
          <div className="flex flex-wrap gap-4">
            <Button asChild className="bg-blue-600 hover:bg-blue-700">
              <Link
                href="https://forms.gle/zUAqmJ4B9fuaUhE89"
                target="_blank"
              >
                Google Form
              </Link>
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
