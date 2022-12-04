package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IUserService userService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            this.queryBlogUser(blog);
            this.isBLogLiked(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result queryBlogById(Long id) {
//        查询blog
        Blog blog = getById(id);
        if (blog == null){
            return Result.fail("笔记不存在");
        }
//        查询blog相关用户
        queryBlogUser(blog);
//        查询此blog是否被用户点赞
        isBLogLiked(blog);
        return Result.ok(blog);
    }

    private void isBLogLiked(Blog blog) {
        //        获取当前用户
        UserDTO user = UserHolder.getUser();
        if (user == null){
//            用户未登录
            return;
        }
        Long userId = user.getId();
//        判断当前用户是否已点赞
        String key = BLOG_LIKED_KEY + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }

    @Override
    public Result likeBlog(Long id) {
//        获取当前用户
        Long userId = UserHolder.getUser().getId();
//        判断当前用户是否已点赞
        String key = BLOG_LIKED_KEY + id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());

        if (score == null){
//            如未点赞则可以点赞
//            数据库点赞数 + 1
            boolean r1 = update().setSql("liked = liked + 1").eq("id", id).update();
//            保存用户到redis set集合中
            if (r1){
                stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
            }
        }else {
//            如已点赞则可以取消点赞
//            数据库点赞数 - 1
            boolean r2 = update().setSql("liked = liked - 1").eq("id", id).update();
//            移除redis set集合中的用户
            if (r2){
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }

        return Result.ok();

    }

    @Override
    public Result queryBlogLikes(Long id) {
        String key = BLOG_LIKED_KEY + id;
//        查询top5的点赞用户
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        if (top5 == null || top5.isEmpty()){
            return Result.ok(Collections.emptyList());
        }
//        解析用户id
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String join = StrUtil.join(",", ids);
//        根据id查询用户
        List<User> users = userService.query().in("id",ids)
                .last("order by field (id," + join + ")").list();
//        转换成userDTO
        List<UserDTO> userDTOS = users.stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
//        返回
        return Result.ok(userDTOS);
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }


}
