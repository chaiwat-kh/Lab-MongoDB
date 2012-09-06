using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

using MvcMongoLab1.Models;

namespace MvcMongoLab1.Controllers
{
    public class HomeController : Controller
    {
        //
        // GET: /Home/

        public ActionResult Index()
        {
            var Web = new WebPageView();
            string text = "";
            string htmlEncoded = Server.HtmlEncode(text);
            Web.Url = text;
            return View(Web);
        }
        [HttpPost]
        public ActionResult Index(WebPageView webPageView)
        {
            var Web = new WebPageView();
            Web = webPageView;

            var html = new HtmlString(Web.Url);
            return View("Detail", Web);
        }

        public ActionResult Detail(WebPageView webPageView)
        {
            return View(webPageView);
        }
    }
}
